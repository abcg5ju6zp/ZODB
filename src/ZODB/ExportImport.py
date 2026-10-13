##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Support for database export and import.

Two export formats are supported:

ZEXP (version 1)
    The historical format: a simple stream of ``oid``/pickle records with
    blob data embedded between markers.  It carries no integrity
    information, so truncation or corruption is typically only noticed
    late, while the two-phase commit machinery is publishing the
    objects.  It is still produced for graphs that do not contain blobs
    and can always be imported.

ZEX2 (version 2)
    A versioned, framed and integrity protected format.  Every record
    carries an explicit boundary marker, lengths and a SHA-256 digest;
    the stream footer contains the record count and a digest covering
    the whole graph, so the importer can validate format and all
    dependencies *before* a single object is published.  Blob payloads
    are part of the record frame they belong to, i.e. object records,
    references and blob data share one export identity.

    Imports of a ZEX2 stream are driven by a persistent progress
    directory: confirmed OID mappings and fully staged records survive
    interrupted attempts and process restarts, so retrying the same
    export reuses the confirmed mappings (no duplicate objects) and can
    continue at the last confirmed record boundary.  Once an export has
    been committed successfully, importing it again returns the
    original root object.
"""

import errno
import hashlib
import logging
import os
import shutil
import tempfile
from io import BytesIO
from tempfile import TemporaryFile

from ZODB._compat import dumps as _journal_dumps
from ZODB._compat import loads as _journal_loads
from ZODB._compat import PersistentPickler
from ZODB._compat import Unpickler
from ZODB._compat import _protocol
from ZODB.blob import Blob
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import ExportError
from ZODB.POSException import POSKeyError
from ZODB.serialize import referencesf
from ZODB.utils import cp
from ZODB.utils import mktemp
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64


logger = logging.getLogger('ZODB.ExportImport')

# ---------------------------------------------------------------------------
# Format constants
# ---------------------------------------------------------------------------

EXPORT_MAGIC_V1 = b'ZEXP'
EXPORT_MAGIC_V2 = b'ZEX2'

FORMAT_ZEXP = 'zexp'
FORMAT_ZEXP2 = 'zexp2'
FORMAT_VERSION_2 = 2

_DIGEST_SIZE = hashlib.sha256().digest_size  # 32

# A framed record: marker | oid | flags | pickle_len | blob_len |
#                  pickle | blob | digest
_RECORD_MARKER = b'ZEX2REC!'
_END_MARKER_V2 = b'ZEX2END!'
_FOOTER_DONE = b'DONE'
_FLAG_BLOB = 1

# Persistent progress journal framing.  The journal is append only;
# every frame is flushed and fsynced before the next one is written.
_JOURNAL_FRAME = b'ZJ'
_J_HEADER = b'H'       # export identity: pickled dict
_J_MAP = b'M'          # confirmed oid mapping: 16 bytes (src, dst)
_J_RECORD = b'E'       # confirmed staged record header (72 bytes)
_J_PREPARED = b'P'     # validation finished: 8 byte record count
_J_COMMITTED = b'C'    # publish finished: 8 byte root dst oid + 8 byte tid

_PROGRESS_DIRNAME = 'zodb-imports'
_STAGE_DIRNAME = 'stage'
_LOCK_FILENAME = 'lock'
_JOURNAL_FILENAME = 'journal'


def _as_bytes(oid):
    if not isinstance(oid, bytes):
        # this happens when all bytes in the oid are < 0x80
        oid = oid.encode('ascii')
    return oid


def _read_exact(f, size, what, bufsize=64 * 1024):
    """Read exactly *size* bytes from *f*, streaming in chunks.

    Raises `ExportError` on a short read, which means the export was
    truncated.
    """
    chunks = []
    remaining = size
    while remaining:
        n = bufsize if bufsize < remaining else remaining
        data = f.read(n)
        if not data:
            break
        chunks.append(data)
        remaining -= len(data)
    if remaining:
        raise ExportError(
            "Truncated export file while reading %s "
            "(missing %d of %d bytes)" % (what, remaining, size))
    if len(chunks) == 1:
        return chunks[0]
    return b''.join(chunks)


def _stream(f, length, sinks, bufsize):
    """Copy *length* bytes from *f* to every file in *sinks*, in chunks.

    Every sink gets every chunk (typically a staging file and/or a
    hasher).  Raises `ExportError` on a short read.
    """
    remaining = length
    while remaining:
        n = bufsize if bufsize < remaining else remaining
        data = f.read(n)
        if not data:
            raise ExportError(
                "Truncated export file "
                "(missing %d of %d bytes of record data)"
                % (remaining, length))
        for sink in sinks:
            sink(data)
        remaining -= len(data)


# Indirection so callers (and tests) can simulate staging failures.
_open = open


class ExportImport:

    def exportFile(self, oid, f=None, bufsize=64 * 1024, format=None):
        if f is None:
            f = TemporaryFile(prefix="EXP")
        elif isinstance(f, str):
            f = open(f, 'w+b')

        if format is None:
            # Preserve the historical behaviour for ordinary graphs
            # without blobs and select the protected streaming format
            # automatically for the graphs that need it.
            if IBlobStorage.providedBy(self._storage):
                entries = self._exportScan(oid)
                if any(entry[2] for entry in entries):
                    return self._exportFileV2(oid, f, bufsize, entries)
            return self._exportFileV1(oid, f, bufsize)

        if format in (1, '1', FORMAT_ZEXP, EXPORT_MAGIC_V1, 'ZEXP'):
            return self._exportFileV1(oid, f, bufsize)
        if format in (2, '2', FORMAT_ZEXP2, EXPORT_MAGIC_V2, 'ZEXP2'):
            return self._exportFileV2(oid, f, bufsize)
        raise ValueError("Unknown export format: %r" % (format,))

    # -- legacy exporter (ZEXP) -------------------------------------------

    def _exportFileV1(self, oid, f, bufsize):
        f.write(EXPORT_MAGIC_V1)
        oids = [oid]
        done_oids = {}
        load = self._storage.load
        supports_blobs = IBlobStorage.providedBy(self._storage)
        while oids:
            oid = oids.pop(0)
            if oid in done_oids:
                continue
            done_oids[oid] = True
            try:
                p, serial = load(oid)
            except:  # noqa: E722 do not use bare 'except'
                logger.debug("broken reference for oid %s", repr(oid),
                             exc_info=True)
            else:
                referencesf(p, oids)
                f.writelines([oid, p64(len(p)), p])

                if supports_blobs:
                    if not isinstance(self._reader.getGhost(p), Blob):
                        continue  # not a blob

                    blobfilename = self._storage.loadBlob(oid, serial)
                    f.write(blob_begin_marker)
                    f.write(p64(os.stat(blobfilename).st_size))
                    blobdata = open(blobfilename, "rb")
                    cp(blobdata, f, bufsize=bufsize)
                    blobdata.close()

        f.write(export_end_marker)
        return f

    # -- new exporter (ZEX2) ----------------------------------------------

    def _exportScan(self, root_oid):
        """Walk the export closure and describe every reachable record.

        Returns a list of ``(oid, serial, is_blob)`` in traversal
        order.
        """
        load = self._storage.load
        supports_blobs = IBlobStorage.providedBy(self._storage)
        oids = [root_oid]
        done_oids = {}
        entries = []
        while oids:
            oid = oids.pop(0)
            if oid in done_oids:
                continue
            done_oids[oid] = True
            try:
                p, serial = load(oid)
            except:  # noqa: E722 do not use bare 'except'
                logger.debug("broken reference for oid %s", repr(oid),
                             exc_info=True)
                continue
            is_blob = (supports_blobs
                       and isinstance(self._reader.getGhost(p), Blob))
            referencesf(p, oids)
            entries.append((oid, serial, is_blob))
        return entries

    def _exportFileV2(self, root_oid, f, bufsize, entries=None):
        if entries is None:
            entries = self._exportScan(root_oid)
        load = self._storage.load
        supports_blobs = IBlobStorage.providedBy(self._storage)

        export_id = os.urandom(16).hex().encode('ascii')
        graph_digest = hashlib.sha256()
        graph_digest.update(EXPORT_MAGIC_V2)
        graph_digest.update(p64(FORMAT_VERSION_2))
        graph_digest.update(export_id)
        graph_digest.update(root_oid)

        header = (
            p64(FORMAT_VERSION_2)
            + p64(len(export_id)) + export_id
            + root_oid
            + p64(0)  # reserved flags
        )
        header_digest = hashlib.sha256(EXPORT_MAGIC_V2 + header).digest()
        f.write(EXPORT_MAGIC_V2)
        f.write(header)
        f.write(header_digest)

        count = 0
        for oid, serial, is_blob in entries:
            # Re-load the pickle; the scan only kept metadata so that
            # large graphs do not have to be held in memory.
            p, serial = load(oid)
            blob_len = 0
            blob_file = None
            if is_blob:
                if not supports_blobs:
                    raise ExportError(
                        "Object %s is a blob but the storage does not "
                        "support blobs" % oid)
                blobfilename = self._storage.loadBlob(oid, serial)
                blob_len = os.stat(blobfilename).st_size
                blob_file = open(blobfilename, "rb")

            flags = _FLAG_BLOB if is_blob else 0
            head = oid + p64(flags) + p64(len(p)) + p64(blob_len)
            record_digest = hashlib.sha256()
            record_digest.update(_RECORD_MARKER)
            record_digest.update(head)
            f.write(_RECORD_MARKER)
            f.write(head)

            # Stream the pickle in bounded chunks instead of writing
            # the whole byte string at once.
            for pos in range(0, len(p), bufsize):
                chunk = p[pos:pos + bufsize]
                f.write(chunk)
                record_digest.update(chunk)

            try:
                if blob_file is not None:
                    copied = [0]

                    def sink(data, _copied=copied, _digest=record_digest,
                             _write=f.write):
                        _copied[0] += len(data)
                        _digest.update(data)
                        _write(data)

                    _stream(blob_file, blob_len, [sink], bufsize)
                    if copied[0] != blob_len:
                        raise ExportError(
                            "Blob for object %s changed size during export"
                            % oid)
            finally:
                if blob_file is not None:
                    blob_file.close()

            digest = record_digest.digest()
            graph_digest.update(digest)
            f.write(digest)
            count += 1

        footer = p64(count) + graph_digest.digest()
        f.write(_END_MARKER_V2)
        f.write(footer)
        f.write(_FOOTER_DONE)
        return f

    # -- import ------------------------------------------------------------

    def importFile(self, f, clue='', customImporters=None, rollback=False):
        # This is tricky, because we need to work in a transaction!

        if isinstance(f, str):
            with open(f, 'rb') as fp:
                return self.importFile(fp, clue=clue,
                                       customImporters=customImporters,
                                       rollback=rollback)

        magic = f.read(4)
        if magic == EXPORT_MAGIC_V1:
            return self._importFileV1(f, clue)
        if magic == EXPORT_MAGIC_V2:
            return self._importFileV2(f, clue, rollback=rollback)

        if customImporters and magic in customImporters:
            f.seek(0)
            return customImporters[magic](self, f, clue)
        raise ExportError("Invalid export header")

    def _importFileV1(self, f, clue):
        t = self.transaction_manager.get()
        if clue:
            t.note(clue)

        return_oid_list = []
        self._import = f, return_oid_list
        self._register()
        t.savepoint(optimistic=True)
        # Return the root imported object.
        if return_oid_list:
            return self.get(return_oid_list[0])
        else:
            return None

    def _importFileV2(self, f, clue, rollback=False):
        # Phase 1: validate the complete stream and stage every record
        # and its blob data *before* anything is published.  No object
        # reaches the storage until this succeeds.
        plan = ImportPlan.prepare(
            self._storage, f, bufsize=self._import_bufsize,
            rollback=rollback)

        if plan.state == STATE_COMMITTED:
            # The export has already been imported; hand back the very
            # same root object instead of creating duplicate objects.
            return self.get(plan.root_dst)

        t = self.transaction_manager.get()
        if clue:
            t.note(clue)

        # Phase 2 happens during the (optimistic) savepoint below and is
        # ultimately published by the surrounding transaction: one
        # transaction publishes the complete, remapped object graph.
        self._import = plan, None
        self._import_plan = plan
        self._register()
        t.savepoint(optimistic=True)
        return self.get(plan.root_dst)

    _import_bufsize = 64 * 1024

    def _importDuringCommit(self, transaction, f, return_oid_list):
        """Import data during two-phase commit.

        Invoked by the transaction manager mid commit.
        Appends one item, the OID of the first object created,
        to return_oid_list.
        """
        if isinstance(f, ImportPlan):
            f.publish(self, transaction)
            return

        oids = {}

        # IMPORTANT: This code should be consistent with the code in
        # serialize.py. It is currently out of date and doesn't handle
        # weak references.

        def persistent_load(ooid):
            """Remap a persistent id to a new ID and create a ghost for it."""

            klass = None
            if isinstance(ooid, tuple):
                ooid, klass = ooid

            if not isinstance(ooid, bytes):
                assert isinstance(ooid, str)
                # this happens when all bytes in the oid are < 0x80
                ooid = ooid.encode('ascii')

            if ooid in oids:
                oid = oids[ooid]
            else:
                if klass is None:
                    oid = self._storage.new_oid()
                else:
                    oid = self._storage.new_oid(), klass
                oids[ooid] = oid

            return Ghost(oid)

        while 1:
            header = f.read(16)
            if header == export_end_marker:
                break
            if len(header) != 16:
                raise ExportError("Truncated export file")

            # Extract header information
            ooid = header[:8]
            length = u64(header[8:16])
            data = f.read(length)

            if len(data) != length:
                raise ExportError("Truncated export file")

            if oids:
                oid = oids[ooid]
                if isinstance(oid, tuple):
                    oid = oid[0]
            else:
                oids[ooid] = oid = self._storage.new_oid()
                return_oid_list.append(oid)

            if (b'blob' in data and
                    isinstance(self._reader.getGhost(data), Blob)):
                # Blob support

                # Make sure we have a (redundant, overly) blob marker.
                if f.read(len(blob_begin_marker)) != blob_begin_marker:
                    raise ValueError("No data for blob object")

                # Copy the blob data to a temporary file
                # and remember the name
                blob_len = u64(f.read(8))
                blob_filename = mktemp(self._storage.temporaryDirectory())
                blob_file = open(blob_filename, "wb")
                cp(f, blob_file, blob_len)
                blob_file.close()
            else:
                blob_filename = None

            pfile = BytesIO(data)
            unpickler = Unpickler(pfile)
            unpickler.persistent_load = persistent_load

            newp = BytesIO()
            pickler = PersistentPickler(persistent_id, newp, _protocol)

            pickler.dump(unpickler.load())
            pickler.dump(unpickler.load())
            data = newp.getvalue()

            if blob_filename is not None:
                self._storage.storeBlob(oid, None, data, blob_filename,
                                        '', transaction)
            else:
                self._storage.store(oid, None, data, '', transaction)


export_end_marker = b'\377' * 16
blob_begin_marker = b'\000BLOBSTART'


class Ghost:
    __slots__ = ("oid",)

    def __init__(self, oid):
        self.oid = oid


class _PersistentRef:
    """Ghost for list shaped persistent references.

    Those are used for persistent weak references (``'w'``) and
    multi-database references (``'n'``/``'m'``).
    """

    __slots__ = ("kind", "args")

    def __init__(self, kind, args):
        self.kind = kind
        self.args = args


def persistent_id(obj):
    if isinstance(obj, Ghost):
        return obj.oid
    if isinstance(obj, _PersistentRef):
        return [obj.kind, obj.args]


# ---------------------------------------------------------------------------
# Persistent import progress (ZEX2)
# ---------------------------------------------------------------------------

STATE_STAGING = 'staging'
STATE_PREPARED = 'prepared'
STATE_COMMITTED = 'committed'


def _imports_root(storage):
    """Return the durable directory holding import progress for *storage*.

    For blob storages the blob temporary directory survives process
    restarts, which keeps progress next to the uncommitted blob data.
    """
    if IBlobStorage.providedBy(storage):
        base = storage.temporaryDirectory()
    else:
        base = os.path.join(tempfile.gettempdir(),
                            _PROGRESS_DIRNAME + '-' +
                            str(abs(hash(storage.getName()))))
    return os.path.join(base, _PROGRESS_DIRNAME)


class _ProgressLock:
    """Best effort interlock against concurrent imports of one export."""

    def __init__(self, path):
        self._path = path
        self._fd = None

    def __enter__(self):
        try:
            self._fd = os.open(self._path,
                               os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            pid = self._read_pid()
            if pid is not None and _pid_alive(pid):
                raise ExportError(
                    "Export import %r is already in progress"
                    % os.path.basename(os.path.dirname(self._path)))
            # Stale lock left over from a crashed process.
            os.unlink(self._path)
            self._fd = os.open(self._path,
                               os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.write(self._fd, str(os.getpid()).encode('ascii'))
        os.fsync(self._fd)
        return self

    def __exit__(self, *exc):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            os.unlink(self._path)
        except OSError:
            pass
        return False

    def _read_pid(self):
        try:
            with _open(self._path, 'rb') as f:
                return int(f.read().strip() or '0')
        except (OSError, ValueError):
            return None


def _pid_alive(pid):
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class ImportPlan:
    """Persistent book keeping for a single export identity."""

    def __init__(self, storage, export_dir, bufsize):
        self._storage = storage
        self._dir = export_dir
        self._bufsize = bufsize
        self.stage_dir = os.path.join(export_dir, _STAGE_DIRNAME)
        self._journal_path = os.path.join(export_dir, _JOURNAL_FILENAME)

        self.export_id = None
        self.root_src = None
        self.version = None
        self.oidmap = {}
        self.records = {}  # idx -> (oid, flags, plen, blen, digest)
        self._record_order = []
        self.state = STATE_STAGING
        self.root_dst = None
        self._committed_tid = None
        self._journal = None

    # -- filesystem helpers ------------------------------------------------

    def _record_pickle(self, idx):
        return os.path.join(self.stage_dir, 'r%020d.pkl' % idx)

    def _record_blob(self, idx):
        return os.path.join(self.stage_dir, 'r%020d.blob' % idx)

    def _record_publish_blob(self, idx):
        return os.path.join(self.stage_dir, 'r%020d.pub' % idx)

    # -- journal -----------------------------------------------------------

    def close(self):
        if self._journal is not None:
            try:
                self._journal.close()
            except OSError:
                pass
            self._journal = None

    def _open_journal(self):
        if self._journal is None:
            self._journal = _open(self._journal_path, 'a+b')
        return self._journal

    def _append(self, kind, payload=b''):
        f = self._open_journal()
        f.seek(0, 2)
        f.write(_JOURNAL_FRAME + kind + p64(len(payload)) + payload)
        f.flush()
        os.fsync(f.fileno())

    def _read_journal(self):
        try:
            with _open(self._journal_path, 'rb') as f:
                data = f.read()
        except FileNotFoundError:
            return []
        frames = []
        pos = 0
        total = len(data)
        while pos < total:
            if data[pos:pos + 2] != _JOURNAL_FRAME:
                raise ExportError("Corrupt import journal for %r"
                                  % self.export_id)
            kind = data[pos + 2:pos + 3]
            length = u64(data[pos + 3:pos + 11])
            payload = data[pos + 11:pos + 11 + length]
            if len(payload) != length:
                raise ExportError("Corrupt import journal for %r"
                                  % self.export_id)
            frames.append((kind, payload))
            pos += 11 + length
        return frames

    def _replay(self, frames):
        map_entries = {}
        for kind, payload in frames:
            if kind == _J_HEADER:
                header = _journal_loads(payload)
                self.version = header['version']
                self.export_id = header['export_id']
                self.root_src = header['root']
            elif kind == _J_MAP:
                src, dst = payload[:8], payload[8:16]
                self.oidmap[src] = dst
            elif kind == _J_RECORD:
                idx = u64(payload[0:8])
                oid = payload[8:16]
                flags = u64(payload[16:24])
                plen = u64(payload[24:32])
                blen = u64(payload[32:40])
                digest = payload[40:72]
                map_entries[idx] = (oid, flags, plen, blen, digest)
            elif kind == _J_PREPARED:
                self.state = STATE_PREPARED
            elif kind == _J_COMMITTED:
                self.state = STATE_COMMITTED
                self.root_dst = payload[:8]
                if len(payload) >= 16:
                    self._committed_tid = payload[8:16]

        if self.state == STATE_COMMITTED:
            # The graph is published; the staging payloads have been
            # removed on purpose and must not be reconstructed.
            return

        for idx in sorted(map_entries):
            oid, flags, plen, blen, digest = map_entries[idx]
            if self._stage_complete(idx, flags, plen, blen):
                self.records[idx] = (oid, flags, plen, blen, digest)
                self._record_order.append(idx)
            else:
                # The payload files for this record were not durable
                # (a crash interrupted the staging); everything after
                # it is unconfirmed as well.  Drop the tail of the
                # journal so the records can be staged again.
                self._truncate_journal_at(idx)
                self.state = STATE_STAGING
                break

    def _truncate_journal_at(self, idx):
        """Remove journal frames describing *idx* or later records.

        Mapping frames are kept (an allocated OID stays allocated).
        """
        f = self._open_journal()
        f.seek(0)
        data = f.read()
        out = bytearray()
        pos = 0
        total = len(data)
        while pos < total:
            kind = data[pos + 2:pos + 3]
            length = u64(data[pos + 3:pos + 11])
            end = pos + 11 + length
            keep = True
            if kind == _J_RECORD:
                frame_idx = u64(data[pos + 11:pos + 19])
                keep = frame_idx < idx
            elif kind in (_J_PREPARED, _J_COMMITTED):
                keep = False
            if keep:
                out.extend(data[pos:end])
            pos = end
        f.seek(0)
        f.truncate()
        f.write(bytes(out))
        f.flush()
        os.fsync(f.fileno())
        self.records.pop(idx, None)
        if idx in self._record_order:
            self._record_order.remove(idx)
        if self.state == STATE_COMMITTED:
            self.state = STATE_STAGING
            self.root_dst = None

    def _stage_complete(self, idx, flags, plen, blen):
        pkl = self._record_pickle(idx)
        try:
            if os.path.getsize(pkl) != plen:
                return False
            if flags & _FLAG_BLOB:
                if os.path.getsize(self._record_blob(idx)) != blen:
                    return False
        except OSError:
            return False
        return True

    # -- lifecycle ---------------------------------------------------------

    @classmethod
    def open(cls, storage, export_id, bufsize=64 * 1024, create=False):
        """Open the persisted progress for *export_id* on *storage*."""
        export_dir = os.path.join(_imports_root(storage), str(export_id))
        if not create and not os.path.isdir(export_dir):
            return None
        os.makedirs(os.path.join(export_dir, _STAGE_DIRNAME), exist_ok=True)
        plan = cls(storage, export_dir, bufsize)
        plan._replay(plan._read_journal())
        return plan

    @classmethod
    def prepare(cls, storage, f, bufsize=64 * 1024, rollback=False):
        """Validate and stage *f*; return a ready to publish plan."""
        header = _read_v2_header(f)
        export_id = header['export_id']

        root_dir = _imports_root(storage)
        export_dir = os.path.join(root_dir, str(export_id))

        if rollback and os.path.isdir(export_dir):
            # Explicit rollback: discard all unpublished progress so
            # the import starts with a fresh OID allocation.
            shutil.rmtree(export_dir)

        os.makedirs(root_dir, exist_ok=True)
        os.makedirs(os.path.join(export_dir, _STAGE_DIRNAME), exist_ok=True)

        with _ProgressLock(os.path.join(export_dir, _LOCK_FILENAME)):
            plan = cls(storage, export_dir, bufsize)
            try:
                plan._replay(plan._read_journal())
                plan._verify_identity(header)

                if plan.state == STATE_COMMITTED:
                    if plan._root_is_present():
                        plan.close()
                        return plan
                    # The marker survived but the actual data is gone;
                    # treat the progress as stale and start over.
                    plan._discard()
                    plan = cls(storage, export_dir, bufsize)
                    os.makedirs(plan.stage_dir, exist_ok=True)
                elif plan.oidmap.get(plan.root_src) is not None \
                        and plan._root_is_present():
                    # A previous attempt actually became visible (the
                    # process may have died before the committed marker
                    # was written): reconcile instead of republishing.
                    plan.root_dst = plan.oidmap[plan.root_src]
                    plan._append(_J_COMMITTED,
                                 plan.root_dst + b'\0' * 8)
                    plan.state = STATE_COMMITTED
                    plan.close()
                    return plan
                # Otherwise the publish never became visible; the
                # durable mappings allow republishing without
                # duplicates.

                if not plan.export_id:
                    plan._init_identity(header)

                plan._stage_stream(f, header)
            except BaseException:
                # Keep the durable progress, but do not hold the
                # journal (or any staging file) open across the failed
                # attempt so a retry can take over cleanly.
                plan.close()
                raise
            return plan

    def _discard(self):
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        shutil.rmtree(self._dir, ignore_errors=True)

    def _verify_identity(self, header):
        if self.export_id is None:
            return
        if (self.export_id != header['export_id']
                or self.root_src != header['root']
                or self.version != header['version']):
            raise ExportError(
                "Import progress does not match the export stream")

    def _init_identity(self, header):
        self.version = header['version']
        self.export_id = header['export_id']
        self.root_src = header['root']
        self._append(_J_HEADER, _journal_dumps({
            'version': header['version'],
            'export_id': header['export_id'],
            'root': header['root'],
        }))

    def _root_is_present(self):
        root_dst = self.root_dst or self.oidmap.get(self.root_src)
        if root_dst is None:
            return False
        try:
            self._storage.load(root_dst)
        except POSKeyError:
            return False
        except Exception:  # noqa: E722 do not use bare 'except'
            logger.debug("could not probe for previously imported root",
                         exc_info=True)
            return False
        return True

    # -- validation/staging ------------------------------------------------

    def _allocate(self, src):
        """Return the confirmed target OID for *src*, allocating if new."""
        dst = self.oidmap.get(src)
        if dst is not None:
            return dst
        used = set(self.oidmap.values())
        # The root record of the database itself always lives at oid
        # 0 and must not be reused for imported objects.
        used.add(z64)
        while True:
            dst = self._storage.new_oid()
            if dst not in used:
                break
        used.add(dst)
        self.oidmap[src] = dst
        self._append(_J_MAP, src + dst)
        return dst

    def _stage_stream(self, f, header):
        already = set(self.records)
        idx = 0
        present = set()
        record_digests = []

        while True:
            marker = _read_exact(f, len(_RECORD_MARKER), "record boundary",
                                 self._bufsize)
            if marker == _END_MARKER_V2:
                break
            if marker != _RECORD_MARKER:
                raise ExportError(
                    "Invalid record boundary at record %d" % idx)

            head = _read_exact(f, 32, "record header", self._bufsize)
            oid = head[0:8]
            flags = u64(head[8:16])
            plen = u64(head[16:24])
            blen = u64(head[24:32])

            payload_digest = hashlib.sha256()
            payload_digest.update(_RECORD_MARKER)
            payload_digest.update(head)

            def sink(data, _digest=payload_digest):
                _digest.update(data)

            if idx in already:
                # Record confirmed during an earlier attempt: verify
                # the stream still matches at this boundary and skip
                # the payload.
                stored = self.records[idx]
                if (stored[0] != oid or stored[1] != flags
                        or stored[2] != plen or stored[3] != blen):
                    raise ExportError(
                        "Export stream does not match the confirmed "
                        "record boundary at record %d" % idx)
                _stream(f, plen + blen, [sink], self._bufsize)
                digest = _read_exact(f, _DIGEST_SIZE, "record digest",
                                     self._bufsize)
                if digest != stored[4] or digest != payload_digest.digest():
                    raise ExportError(
                        "Integrity check failed for record %d" % idx)
                record_digests.append(digest)
                present.add(oid)
                idx += 1
                continue

            if oid in present:
                raise ExportError(
                    "Duplicate record for object %s in export" % oid)

            pkl_path = self._record_pickle(idx)
            blob_path = self._record_blob(idx)
            pkl_file = blob_file = None
            try:
                try:
                    pkl_file = _open(pkl_path, 'wb')

                    def pkl_sink(data, _out=pkl_file,
                                 _digest=payload_digest):
                        _out.write(data)
                        _digest.update(data)

                    _stream(f, plen, [pkl_sink], self._bufsize)
                    pkl_file.flush()
                    os.fsync(pkl_file.fileno())

                    if flags & _FLAG_BLOB:
                        if not IBlobStorage.providedBy(self._storage):
                            raise ExportError(
                                "Export contains blob data but the target "
                                "storage does not support blobs")
                        blob_file = _open(blob_path, 'wb')

                        def blob_sink(data, _out=blob_file,
                                      _digest=payload_digest):
                            _out.write(data)
                            _digest.update(data)

                        _stream(f, blen, [blob_sink], self._bufsize)
                        blob_file.flush()
                        os.fsync(blob_file.fileno())
                except OSError as e:
                    raise ExportError(
                        "Failed to stage blob/object data for record %d: %s"
                        % (idx, e))
            except BaseException:
                # Cleaning up an unpublished, partially staged record;
                # every confirmed record (and its OID mapping) is kept
                # for resume.
                for fh in (pkl_file, blob_file):
                    if fh is not None:
                        fh.close()
                for path in (pkl_path, blob_path):
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                raise
            else:
                pkl_file.close()
                if blob_file is not None:
                    blob_file.close()

            digest = _read_exact(f, _DIGEST_SIZE, "record digest",
                                 self._bufsize)
            if digest != payload_digest.digest():
                for path in (pkl_path, blob_path):
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                raise ExportError(
                    "Integrity check failed for record %d" % idx)

            # The payload is complete and durable; now confirm the
            # OID mapping and the record boundary.
            self._allocate(oid)
            record_payload = (p64(idx) + oid + p64(flags) + p64(plen)
                              + p64(blen) + digest)
            self._append(_J_RECORD, record_payload)
            self.records[idx] = (oid, flags, plen, blen, digest)
            self._record_order.append(idx)
            record_digests.append(digest)
            present.add(oid)
            idx += 1

        # Footer: count and whole graph integrity.
        count = u64(_read_exact(f, 8, "record count", self._bufsize))
        graph_digest = _read_exact(f, _DIGEST_SIZE, "graph digest",
                                   self._bufsize)
        done = _read_exact(f, len(_FOOTER_DONE), "footer", self._bufsize)
        if done != _FOOTER_DONE:
            raise ExportError("Truncated export file (missing footer)")
        if count != idx:
            raise ExportError(
                "Export record count mismatch: footer says %d, stream has %d"
                % (count, idx))

        if self.root_src not in present:
            raise ExportError(
                "Export root object %s is missing from the export"
                % self.root_src)

        expected = hashlib.sha256()
        expected.update(EXPORT_MAGIC_V2)
        expected.update(p64(header['version']))
        expected.update(header['export_id'].encode(
            'ascii') if isinstance(header['export_id'], str)
            else header['export_id'])
        expected.update(self.root_src)
        for digest in record_digests:
            expected.update(digest)
        if graph_digest != expected.digest():
            raise ExportError("Whole graph integrity check failed")

        # Dependency resolution: every strong reference either points
        # at a record contained in the export or gets a fresh, dangling
        # target OID, matching the historical import semantics.  Weak
        # and multi-database references are left untouched when their
        # targets are not part of the export.
        for idx in sorted(self.records):
            oid, flags, plen, blen, digest = self.records[idx]
            with _open(self._record_pickle(idx), 'rb') as p:
                pickle_bytes = p.read()
            for ref in referencesf(pickle_bytes):
                ref = _as_bytes(ref)
                self._allocate(ref)

        self.root_dst = self.oidmap[self.root_src]
        if self.state != STATE_PREPARED:
            self._append(_J_PREPARED, p64(count))
            self.state = STATE_PREPARED

    # -- publish -----------------------------------------------------------

    def publish(self, connection, transaction):
        """Publish the complete, remapped graph in one transaction.

        Invoked from the connection while the transaction commits (via
        an optimistic savepoint).  Nothing here reads the original
        export stream; everything comes from the durable staging area.
        """
        if self.state not in (STATE_PREPARED, STATE_COMMITTED):
            raise ExportError(
                "Export %r cannot be published in state %s"
                % (self.export_id, self.state))

        oids = dict(self.oidmap)
        in_graph = {oid for oid, _, _, _, _ in self.records.values()}

        def persistent_load(ref):
            return self._persistent_load(ref, oids, in_graph)

        publish_blobs = []
        try:
            for idx in sorted(self.records):
                src_oid, flags, plen, blen, digest = self.records[idx]
                dst_oid = oids[src_oid]
                with _open(self._record_pickle(idx), 'rb') as p:
                    data = p.read()

                pfile = BytesIO(data)
                unpickler = Unpickler(pfile)
                unpickler.persistent_load = persistent_load

                newp = BytesIO()
                pickler = PersistentPickler(persistent_id, newp, _protocol)
                pickler.dump(unpickler.load())
                pickler.dump(unpickler.load())
                data = newp.getvalue()

                if flags & _FLAG_BLOB:
                    # Hand a copy to the storage: savepoint storage may
                    # rename the file away, and a failed attempt must
                    # leave the canonical staged blob intact for retry.
                    pub = self._record_publish_blob(idx)
                    shutil.copyfile(self._record_blob(idx), pub)
                    publish_blobs.append(pub)
                    connection._storage.storeBlob(
                        dst_oid, None, data, pub, '', transaction)
                else:
                    connection._storage.store(
                        dst_oid, None, data, '', transaction)
        except BaseException:
            self._cleanup_publish_blobs(publish_blobs)
            raise

    def _persistent_load(self, ref, oids, in_graph):
        if isinstance(ref, list):
            # Weak ('w') or multi database ('n'/'m') references.
            kind, args = ref
            args = list(args)
            oid_pos = 1 if kind in ('n', 'm') else 0
            oid = _as_bytes(args[oid_pos])
            if oid in oids:
                args[oid_pos] = oids[oid]
            elif kind == 'w':
                # The weak target is not part of the export graph.  Map
                # it to an OID that is never a reachable object in the
                # target graph, so the weak ref resolves to None instead
                # of to a ghost that would raise on attribute access.
                # FileStorage-style counters start at the root OID and
                # grow upwards, so an all-one OID is guaranteed to be
                # absent.  This mirrors the ZODB semantic that weak refs
                # never force objects to exist.
                args[oid_pos] = b'\xff' * 8
            return _PersistentRef(kind, tuple(args))

        klass = None
        if isinstance(ref, tuple):
            oid, klass = ref
        else:
            oid = ref
        oid = _as_bytes(oid)

        if oid in oids:
            new_oid = oids[oid]
        else:
            # Safety net for references not seen while scanning.  The
            # allocation is journaled just like any other.
            new_oid = self._allocate(oid)
            oids[oid] = new_oid

        if klass is None:
            return Ghost(new_oid)
        return Ghost((new_oid, klass))

    def _cleanup_publish_blobs(self, paths):
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass

    def publish_failed(self):
        """Forget an unpublished publish attempt; keep progress."""
        try:
            names = os.listdir(self.stage_dir)
        except OSError:
            names = ()
        for name in names:
            if name.endswith('.pub'):
                try:
                    os.unlink(os.path.join(self.stage_dir, name))
                except OSError:
                    pass
        self.close()

    def committed(self, tid):
        """Record the successful publish and free unpublished data."""
        if self.state == STATE_COMMITTED:
            return
        self._append(_J_COMMITTED, self.root_dst + (tid or b'\0' * 8))
        self.state = STATE_COMMITTED
        self._committed_tid = tid
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        # The blobs and pickles are now published data; only the small
        # journal needs to stay around to make repeated imports return
        # the original root.
        shutil.rmtree(self.stage_dir, ignore_errors=True)


def _read_v2_header(f):
    version = u64(_read_exact(f, 8, "format version"))
    if version != FORMAT_VERSION_2:
        raise ExportError("Unsupported export format version %d" % version)
    eid_len = u64(_read_exact(f, 8, "export id length"))
    if not eid_len or eid_len > 256:
        raise ExportError("Invalid export id length %d" % eid_len)
    export_id = _read_exact(f, eid_len, "export id")
    try:
        export_id_text = export_id.decode('ascii')
    except UnicodeDecodeError:
        raise ExportError("Invalid export id")
    root = _read_exact(f, 8, "root oid")
    flags = u64(_read_exact(f, 8, "header flags"))
    digest = _read_exact(f, _DIGEST_SIZE, "header digest")
    expected = hashlib.sha256()
    expected.update(EXPORT_MAGIC_V2)
    expected.update(p64(version))
    expected.update(p64(eid_len))
    expected.update(export_id)
    expected.update(root)
    expected.update(p64(flags))
    if digest != expected.digest():
        raise ExportError("Export header integrity check failed")
    return {
        'version': version,
        'export_id': export_id_text,
        'root': root,
        'flags': flags,
    }


def readExportHeader(f):
    """Read and validate an export header from a seekable stream.

    Returns a dict with ``format`` (``1`` or ``2``) and, for version 2,
    ``version``/``export_id``/``root``.  The stream position is left
    after the magic for format 1 and after the complete header for
    format 2.
    """
    magic = f.read(4)
    if magic == EXPORT_MAGIC_V1:
        return {'format': 1}
    if magic == EXPORT_MAGIC_V2:
        header = _read_v2_header(f)
        header['format'] = 2
        return header
    raise ExportError("Invalid export header")
