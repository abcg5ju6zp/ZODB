##############################################################################
#
# Copyright (c) 2026 Zope Foundation and Contributors.
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
"""Tests for the versioned, integrity protected export format (ZEX2)."""
import os
from io import BytesIO
from unittest import mock

import transaction
from persistent.mapping import PersistentMapping
from persistent.wref import WeakRef

import ZODB
from ZODB import ExportImport
from ZODB.blob import Blob
from ZODB.blob import BlobStorage
from ZODB.ExportImport import EXPORT_MAGIC_V1
from ZODB.ExportImport import EXPORT_MAGIC_V2
from ZODB.ExportImport import ImportPlan
from ZODB.ExportImport import readExportHeader
from ZODB.FileStorage import FileStorage
from ZODB.POSException import ExportError
from ZODB.POSException import POSKeyError
from ZODB.POSException import StorageTransactionError
from ZODB.tests.MinPO import MinPO
from ZODB.tests.util import TestCase
from ZODB.utils import z64


class VoteFailureBlobStorage(BlobStorage):
    # Blob storage that fails its next *tpc_vote* calls on demand to
    # simulate storage failures during two-phase commit.
    fail_votes = 0

    def tpc_vote(self, transaction):
        if self.fail_votes:
            self.fail_votes -= 1
            raise StorageTransactionError("injected vote failure")
        return self._BlobStorage__storage.tpc_vote(transaction)


class ExportImportV2Tests(TestCase):

    def setUp(self):
        super().setUp()
        self._databases = []

    def tearDown(self):
        # Databases have to be closed while their connections are
        # still registered as transaction synchronizers (the standard
        # tear down clears them afterwards).
        for db in reversed(self._databases):
            try:
                db.close()
            except Exception:  # pragma: no cover
                pass
        self._databases = []
        super().tearDown()

    def make_db(self, name, storage_wrapper=None):
        fs_path = os.path.join(os.getcwd(), name + '.fs')
        blob_dir = os.path.join(os.getcwd(), name + '-blobs')
        storage = (storage_wrapper or BlobStorage)(
            blob_dir, FileStorage(fs_path))
        db = ZODB.DB(storage)
        self._databases.append(db)
        return db

    def make_source(self, db=None, weak=False, dangling=False,
                    blob_sizes=(200000, 50000, 300)):
        db = db or self.make_db('source')
        conn = db.open()
        blobs = []
        for size in blob_sizes:
            blob = Blob()
            with blob.open('w') as fp:
                fp.write(b'x' * size if size == blob_sizes[0]
                         else bytes([65 + blobs.__len__()]) * size)
            blobs.append(blob)

        target = MinPO(7)
        ref = WeakRef(target)

        container = PersistentMapping()
        for i, blob in enumerate(blobs):
            container['blob%d' % i] = blob
        if weak:
            container['target'] = target
            container['ref'] = ref
        conn.root()['container'] = container

        if dangling:
            # Commit the target separately, then build a second graph
            # that reaches it *only* through a persistent weakref.
            conn.root()['target'] = target
            transaction.commit()
            other = PersistentMapping()
            other['blob'] = blobs[0]
            other['ref'] = ref
            transaction.begin()
            conn.root()['other'] = other
            transaction.commit()
            return conn, other, blobs, target

        transaction.commit()
        return conn, container, blobs, target

    def export(self, conn, oid, path='export.zexp', **kw):
        path = os.path.join(os.getcwd(), path)
        f = conn.exportFile(oid, path, **kw)
        assert f is not None
        f.close()
        return path

    def read_bytes(self, path):
        with open(path, 'rb') as f:
            return f.read()

    def write_bytes(self, path, data):
        with open(path, 'wb') as f:
            f.write(data)

    def open_plan(self, db, path):
        with open(path, 'rb') as f:
            header = readExportHeader(f)
        return ImportPlan.open(db.storage, header['export_id'])

    def import_graph(self, db, path, key='container', root=None, **kw):
        conn = db.open()
        transaction.begin()
        data = conn.importFile(path, **kw)
        conn.root()[key] = data
        transaction.commit()
        conn.close()
        return data

    def test_auto_format_selection(self):
        # Graphs with blobs use the new format, blob-free graphs keep
        # producing the historical ZEXP stream.
        db1 = self.make_db('source')
        conn = db1.open()
        plain = PersistentMapping({'a': PersistentMapping()})
        conn.root()['plain'] = plain
        with_blobs = PersistentMapping({'blob': Blob()})
        with with_blobs['blob'].open('w') as fp:
            fp.write(b'payload')
        conn.root()['with_blobs'] = with_blobs
        transaction.commit()

        p1 = self.export(conn, plain._p_oid, 'plain.zexp')
        self.assertEqual(self.read_bytes(p1)[:4], EXPORT_MAGIC_V1)
        p2 = self.export(conn, with_blobs._p_oid, 'blobs.zexp')
        self.assertEqual(self.read_bytes(p2)[:4], EXPORT_MAGIC_V2)

    def test_forced_format_selection(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)

        legacy = self.export(conn, container._p_oid, 'legacy.zexp',
                             format='zexp')
        self.assertEqual(self.read_bytes(legacy)[:4], EXPORT_MAGIC_V1)
        modern = self.export(conn, container._p_oid, 'modern.zexp',
                             format=2)
        self.assertEqual(self.read_bytes(modern)[:4], EXPORT_MAGIC_V2)

        # A blob-free graph can be exported in the new format as well.
        plain = PersistentMapping({'a': 1})
        conn.root()['plain'] = plain
        transaction.commit()
        p = self.export(conn, plain._p_oid, 'plain-v2.zexp',
                        format='zexp2')
        header = readExportHeader(BytesIO(self.read_bytes(p)))
        self.assertEqual(header['format'], 2)
        self.assertEqual(header['version'], 2)
        self.assertEqual(len(header['export_id']), 32)

    def test_blob_roundtrip(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        self.import_graph(db2, path)
        conn2 = db2.open()
        imported = conn2.root()['container']
        for i, original in enumerate(self.read_source_blobs(db1)):
            with imported['blob%d' % i].open() as fp:
                self.assertEqual(fp.read(), original)

    def read_source_blobs(self, db):
        conn = db.open()
        container = conn.root()['container']
        payloads = []
        for key in sorted(container.keys()):
            if key.startswith('blob'):
                with container[key].open() as fp:
                    payloads.append(fp.read())
        return payloads

    def test_streaming_small_bufsize(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(
            db1, blob_sizes=(100000, 70000))
        path = self.export(conn, container._p_oid, bufsize=4096)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        conn2._import_bufsize = 4096
        transaction.begin()
        data = conn2.importFile(path)
        conn2.root()['container'] = data
        transaction.commit()
        fresh = db2.open()
        imported = fresh.root()['container']
        with imported['blob0'].open() as fp:
            self.assertEqual(fp.read(), b'x' * 100000)
        with imported['blob1'].open() as fp:
            self.assertEqual(fp.read(), b'B' * 70000)

    def test_weakref_is_remapped(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1, weak=True)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        self.import_graph(db2, path)
        conn2 = db2.open()
        imported = conn2.root()['container']
        # The weak reference follows the remapped OID: it resolves to
        # the imported target rather than dangling.
        self.assertIs(imported['ref'](), imported['target'])
        self.assertEqual(imported['target'].value, 7)
        with imported['blob0'].open() as fp:
            self.assertEqual(len(fp.read()), 200000)

    def test_weakref_target_not_in_export_dangles(self):
        db1 = self.make_db('source')
        conn = db1.open()
        # The export closure contains a weak reference whose target is
        # stored separately and is *not* reached from the export root.
        target = MinPO(7)
        conn.root()['target'] = target
        blob = Blob()
        with blob.open('w') as fp:
            fp.write(b'blobdata')
        other = PersistentMapping({'blob': blob})
        conn.root()['other'] = other
        transaction.commit()

        # Attach the weak ref after the target has an OID so the
        # reference is serialized as a weak reference.
        ref = WeakRef(target)
        transaction.begin()
        other['ref'] = ref
        transaction.commit()
        # Force a fresh view so `other` carries its committed OID.
        conn.close()
        conn = db1.open()
        other = conn.root()['other']

        path = self.export(conn, other._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        imported = conn2.importFile(path)
        conn2.root()['other'] = imported
        transaction.commit()

        conn3 = db2.open()
        imported = conn3.root()['other']
        # The weak target was never part of the export closure; the
        # reference imports as a harmless dangling weakref.
        self.assertIsNone(imported['ref']())
        with imported['blob'].open() as fp:
            fp.read()

    def test_truncated_record_fails_before_publish(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)
        data = self.read_bytes(path)
        truncated = os.path.join(os.getcwd(), 'truncated.zexp')
        self.write_bytes(truncated, data[:len(data) - 5000])

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        with self.assertRaises(ExportError):
            conn2.importFile(truncated)
        transaction.abort()
        # Nothing was registered for publishing.
        self.assertIsNone(conn2._import_plan)
        self.assertIsNone(conn2._import)

        # Retrying with the complete export continues at the confirmed
        # boundary and reuses the confirmed OID mappings.
        plan_before = self.open_plan(db2, path)
        self.assertIn(container._p_oid, plan_before.oidmap)
        mapped = dict(plan_before.oidmap)
        root = self.import_graph(db2, path)
        plan_after = self.open_plan(db2, path)
        self.assertEqual(plan_after.state, 'committed')
        # Every mapping that was confirmed before the truncation is
        # reused unchanged; no duplicates are created on retry.
        for src, dst in mapped.items():
            self.assertEqual(plan_after.oidmap[src], dst)
        self.assertEqual(root._p_oid, mapped[container._p_oid])

    def test_footer_truncation_fails(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(
            db1, blob_sizes=(1234,))
        path = self.export(conn, container._p_oid)
        data = self.read_bytes(path)
        # Cut everything inside the footer.
        truncated = os.path.join(os.getcwd(), 'truncated.zexp')
        self.write_bytes(truncated, data[:len(data) - 10])
        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        with self.assertRaises(ExportError):
            conn2.importFile(truncated)
        transaction.abort()

    def test_corrupt_record_fails_and_recovers(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)
        data = bytearray(self.read_bytes(path))
        # Flip a byte well inside the large first blob payload.
        pos = len(data) // 2
        data[pos] ^= 0xFF
        corrupt = os.path.join(os.getcwd(), 'corrupt.zexp')
        self.write_bytes(corrupt, bytes(data))

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        with self.assertRaises(ExportError):
            conn2.importFile(corrupt)
        transaction.abort()

        mapped_before = dict(self.open_plan(db2, path).oidmap)
        root = self.import_graph(db2, path)
        plan = self.open_plan(db2, path)
        for src, dst in mapped_before.items():
            self.assertEqual(plan.oidmap[src], dst)
        self.assertEqual(root._p_oid, mapped_before[container._p_oid])

    def test_blob_temp_write_failure_cleans_up_and_resumes(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()

        real_open = ExportImport._open
        failures = [0]

        def failing_open(file, *args, **kwargs):
            if isinstance(file, str) and file.endswith('.blob') \
                    and failures[0] < 1:
                failures[0] += 1
                raise OSError("injected staging failure")
            return real_open(file, *args, **kwargs)

        with mock.patch.object(ExportImport, '_open', failing_open):
            with self.assertRaises(ExportError):
                conn2.importFile(path)
        transaction.abort()

        plan = self.open_plan(db2, path)
        # The non-blob container record was confirmed before the blob
        # staging failure; its OID mapping survives.
        self.assertIn(container._p_oid, plan.oidmap)
        confirmed = dict(plan.oidmap)
        # No half staged blob temp file leaked.
        stage = os.path.join(os.path.dirname(plan._journal_path), 'stage')
        self.assertEqual(
            [n for n in os.listdir(stage) if n.endswith('.blob')], [])

        root = self.import_graph(db2, path)
        plan = self.open_plan(db2, path)
        self.assertEqual(plan.state, 'committed')
        for src, dst in confirmed.items():
            self.assertEqual(plan.oidmap[src], dst)
        self.assertEqual(root._p_oid, confirmed[container._p_oid])

    def test_vote_failure_republishes_without_duplicates(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest', storage_wrapper=VoteFailureBlobStorage)
        db2.storage.fail_votes = 1

        conn2 = db2.open()
        transaction.begin()
        data = conn2.importFile(path)
        root_oid = data._p_oid
        conn2.root()['container'] = data
        with self.assertRaises(StorageTransactionError):
            transaction.commit()

        # The publish became visible nowhere.
        with self.assertRaises(POSKeyError):
            db2.storage.load(root_oid)

        # Retry reuses the prepared plan and its OID mappings.
        conn3 = db2.open()
        transaction.begin()
        retry = conn3.importFile(path)
        self.assertEqual(retry._p_oid, root_oid)
        conn3.root()['container'] = retry
        transaction.commit()

        plan = self.open_plan(db2, path)
        self.assertEqual(plan.state, 'committed')
        # Every imported OID shows up exactly once in exactly one
        # transaction; no duplicate objects were created.
        seen = {}
        for txn in db2.storage.iterator():
            for rec in txn:
                seen[rec.oid] = seen.get(rec.oid, 0) + 1
        for dst in plan.oidmap.values():
            self.assertEqual(seen.get(dst), 1)

        fresh = db2.open()
        imported = fresh.root()['container']
        with imported['blob0'].open() as fp:
            self.assertEqual(len(fp.read()), 200000)

    def test_abort_keeps_progress_and_retry_reuses_oids(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        data = conn2.importFile(path)
        root_oid = data._p_oid
        conn2.root()['container'] = data
        transaction.abort()

        with self.assertRaises(POSKeyError):
            db2.storage.load(root_oid)

        conn3 = db2.open()
        transaction.begin()
        retry = conn3.importFile(path)
        self.assertEqual(retry._p_oid, root_oid)
        conn3.root()['container'] = retry
        transaction.commit()
        self.assertEqual(self.open_plan(db2, path).state, 'committed')

    def test_restart_after_prepared_continues(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        data = conn2.importFile(path)
        root_oid = data._p_oid
        # Simulate a process restart before anything was committed.
        db2.close()

        db2 = self.make_db('dest')
        conn3 = db2.open()
        transaction.begin()
        retry = conn3.importFile(path)
        self.assertEqual(retry._p_oid, root_oid)
        conn3.root()['container'] = retry
        transaction.commit()

        fresh = db2.open()
        imported = fresh.root()['container']
        with imported['blob1'].open() as fp:
            self.assertEqual(fp.read(), b'B' * 50000)
        self.assertEqual(self.open_plan(db2, path).state, 'committed')

    def test_restart_after_commit_returns_same_root(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        data = self.import_graph(db2, path)
        root_oid = data._p_oid
        db2.close()

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        again = conn2.importFile(path)
        self.assertEqual(again._p_oid, root_oid)
        # The repeated import publishes nothing.
        self.assertIsNone(conn2._savepoint_storage)
        transaction.commit()

        seen = {}
        for txn in db2.storage.iterator():
            for rec in txn:
                seen[rec.oid] = seen.get(rec.oid, 0) + 1
        plan = self.open_plan(db2, path)
        for dst in plan.oidmap.values():
            self.assertEqual(seen.get(dst), 1)

    def test_rollback_discards_unpublished_progress(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        data = conn2.importFile(path)
        old_oid = data._p_oid
        transaction.abort()

        conn3 = db2.open()
        transaction.begin()
        restarted = conn3.importFile(path, rollback=True)
        self.assertNotEqual(restarted._p_oid, old_oid)
        conn3.root()['container'] = restarted
        transaction.commit()

    def test_legacy_zexp_with_blobs_imports(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid, format='zexp')
        self.assertEqual(self.read_bytes(path)[:4], EXPORT_MAGIC_V1)

        db2 = self.make_db('dest')
        self.import_graph(db2, path)
        imported = db2.open().root()['container']
        with imported['blob0'].open() as fp:
            self.assertEqual(len(fp.read()), 200000)

    def test_legacy_non_blob_graph_keeps_duplicate_semantics(self):
        db1 = self.make_db('source')
        conn = db1.open()
        graph = PersistentMapping()
        for n in range(10):
            graph[n] = PersistentMapping({0: n})
        conn.root()['graph'] = graph
        transaction.commit()
        path = self.export(conn, graph._p_oid)

        db2 = self.make_db('dest')
        first = self.import_graph(db2, path, key='first')
        second = self.import_graph(db2, path, key='second')
        # Historical behaviour: each import allocates fresh OIDs.
        self.assertNotEqual(first._p_oid, second._p_oid)

    def test_custom_importer_unchanged(self):
        db = self.make_db('dest')
        conn = db.open()
        called = []

        def custom(conn, f, clue):
            called.append((f.read(), clue))
            return 'custom-result'

        transaction.begin()
        result = conn.importFile(
            BytesIO(b'CUST' + b'payload'), clue='hi',
            customImporters={b'CUST': custom})
        self.assertEqual(result, 'custom-result')
        self.assertEqual(called, [(b'CUST' + b'payload', 'hi')])
        transaction.abort()

    def test_invalid_header(self):
        db = self.make_db('dest')
        conn = db.open()
        transaction.begin()
        with self.assertRaises(ExportError):
            conn.importFile(BytesIO(b'NOPE' + b'xxxx'))
        transaction.abort()

    def test_unsupported_version(self):
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)
        raw = bytearray(self.read_bytes(path))
        # version field sits right after the 4 byte magic
        raw[4:12] = b'\x00' * 7 + b'\x09'
        broken = os.path.join(os.getcwd(), 'broken.zexp')
        self.write_bytes(broken, bytes(raw))
        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        with self.assertRaises(ExportError):
            conn2.importFile(broken)
        transaction.abort()

    def test_journal_reuses_oid_after_storage_counter_reset(self):
        # Even when the storage OID counter restarts (process restart
        # with an uncommitted allocation), resumed mappings must not
        # collide with freshly allocated OIDs.
        db1 = self.make_db('source')
        conn, container, blobs, target = self.make_source(db1)
        path = self.export(conn, container._p_oid)

        db2 = self.make_db('dest')
        conn2 = db2.open()
        transaction.begin()
        data = conn2.importFile(path)
        root_oid = data._p_oid
        transaction.abort()
        plan = self.open_plan(db2, path)
        used = set(plan.oidmap.values())
        self.assertIn(root_oid, used)

        # Simulate a process restart of the storage's in-memory OID
        # counter: it starts again right after the highest committed
        # OID while the journal still owns the confirmed range.
        file_storage = db2.storage._BlobStorage__storage
        file_storage._oid = z64

        new_oid = plan._allocate(b'\xff' * 8)
        self.assertNotIn(new_oid, used)
        second = plan._allocate(b'\xfe' * 8)
        self.assertNotIn(second, used)
        self.assertNotEqual(second, new_oid)
