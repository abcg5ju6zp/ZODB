"""Adapt IStorage objects to IMVCCStorage

This is a largely internal implementation of ZODB, especially DB and
Connection.  It takes the MVCC implementation involving invalidations
and start time and moves it into a storage adapter.  This allows ZODB
to treat Relstoage and other storages in pretty much the same way and
also simplifies the implementation of the DB and Connection classes.

In addition to MVCC bookkeeping, this module gives each two-phase
commit a *stable identity* and a *recoverable phase*
(:class:`TransactionState`).  The storage-allocated tid, the write
set, blob staging and the invalidation are all bound to the same
transaction state, so when ``tpc_finish`` fails after the storage
committed -- or when its answer is simply lost -- the outcome can be
recovered from storage facts instead of being guessed: a transaction
that the storage reports as committed is completed exactly once and is
never rolled back or written a second time.
"""
import logging
import threading

import zope.interface

from . import POSException
from . import interfaces
from . import serialize
from .utils import Lock
from .utils import RLock
from .utils import oid_repr
from .utils import p64
from .utils import tid_repr
from .utils import u64

logger = logging.getLogger('ZODB.mvccadapter')


def _tid_greater(left, right):
    """Ordering over tids that tolerates non-conforming test doubles.

    Real storages always hand out 8-byte tids; defensive code must not
    blow up when a minimal storage stub passes a non-bytes value.
    """
    try:
        return left > right
    except TypeError:
        return False


#: Attribute under which the recoverable :class:`TransactionState` is
#: attached to the storage-level transaction metadata.  Every layer
#: (Connection, MVCC adapter, blob wrapper) sees the *same* metadata
#: object for one two-phase commit, so this is the stable identity of
#: the transaction.
TXN_STATE_ATTR = '_zodb_txn_state'

# How long abort() waits for a concurrent finish() to reach a verdict
# before falling back to storage facts.
_FINISH_SETTLE_TIMEOUT = 60


class TransactionState:
    """Stable identity and recoverable phase of one two-phase commit.

    The object is shared, via the storage transaction metadata, by the
    connection/cache layer, the MVCC adapter and the blob layer.  It
    records the storage-allocated ``tid``, the write set and which of
    the downstream commit steps (invalidation, cache completion) have
    already been confirmed, so that every step is completed at most
    once even if ``tpc_finish`` raises, the answer is lost, abort races
    with finish, or the committing connection is closed before the
    cleanup ran.
    """

    #: tpc_begin done, stores in progress / vote pending
    BEGIN = 'begin'
    #: tpc_vote returned without error
    VOTED = 'voted'
    #: inside storage tpc_finish, verdict not known yet
    FINISHING = 'finishing'
    #: storage committed; downstream steps complete (or completing)
    COMMITTED = 'committed'
    #: storage aborted; nothing was published
    ABORTED = 'aborted'
    #: finish or vote failed and the storage did not commit
    FAILED = 'failed'

    _PHASES = (BEGIN, VOTED, FINISHING, COMMITTED, ABORTED, FAILED)

    def __init__(self, base, transaction, tid=None):
        self.base = base
        self.transaction = transaction
        # Storage-allocated transaction id.  Known before the finish
        # callback for storages that allocate their tid in tpc_begin
        # (FileStorage, MappingStorage, ...); otherwise captured from
        # the finish callback.
        self.tid = tid
        self.phase = self.BEGIN

        # Reentrrant because several layers may ask for the verdict in
        # the course of one unwinding call chain.
        self.lock = RLock()
        self.settled = threading.Event()

        # True once the storage actually invoked the finish callback.
        # The callback runs at the storage's commit point; once it
        # fired, the transaction must be treated as published.
        self.callback_fired = False

        # Downstream confirmation flags.
        self.invalidated = False
        self.cache_completed = False

        # Exception that made tpc_finish uncertain, if any.
        self.finish_error = None

        # Set when the committing MVCC instance was released while the
        # transaction was still unresolved; the base keeps the state so
        # invalidation recovery can still run.
        self.detached = False

        # Write set of the transaction (oid -> 1), same object as
        # MVCCAdapterInstance._modified while the commit is live.
        self.modified = set()

    # -- phase transitions -------------------------------------------------

    def note_voted(self):
        with self.lock:
            if self.phase == self.BEGIN:
                self.phase = self.VOTED

    def note_vote_failed(self, exc):
        with self.lock:
            if self.phase in (self.BEGIN, self.VOTED):
                self.phase = self.FAILED
                self.settled.set()
                self.base._unregister_state(self)

    def note_finishing(self):
        with self.lock:
            if self.phase in (self.BEGIN, self.VOTED):
                self.phase = self.FINISHING

    def mark_committed(self):
        with self.lock:
            self.phase = self.COMMITTED
            self.settled.set()
            self.base._unregister_state(self)

    def mark_aborted(self):
        with self.lock:
            if self.phase != self.COMMITTED:
                self.phase = self.ABORTED
            self.settled.set()
            self.base._unregister_state(self)

    def mark_failed(self):
        with self.lock:
            if self.phase != self.COMMITTED:
                self.phase = self.FAILED
            self.settled.set()
            self.base._unregister_state(self)

    def is_settled(self):
        return self.phase in (self.COMMITTED, self.ABORTED, self.FAILED)

    def wait_settled(self, timeout=_FINISH_SETTLE_TIMEOUT):
        """Wait for a concurrent finish to reach a verdict."""
        if not self.is_settled():
            self.settled.wait(timeout)
        return self.is_settled()

    def is_committed(self):
        """Return whether the final verdict of this txn is *committed*.

        If a finish is concurrently in progress, wait for its verdict.
        """
        if self.phase == self.FINISHING:
            self.wait_settled()
        return self.phase == self.COMMITTED

    # -- downstream steps --------------------------------------------------

    def note_invalidated(self):
        with self.lock:
            self.invalidated = True

    def note_cache_completed(self):
        with self.lock:
            self.cache_completed = True


def storage_committed(storage, tid, oids):
    """Decide from *storage facts* whether a tentative transaction committed.

    Returns ``True`` (committed), ``False`` (not committed) or ``None``
    (the storage cannot tell).  This function must never write to the
    storage; it only reads facts:

    1. If the storage still holds the transaction in flight, it was not
       finished.

    2. If its last transaction is older than the tentative ``tid`` it
       was not committed (tids are monotonic).

    3. If the last transaction equals ``tid`` it was committed.

    4. If a later transaction exists, one of our object revisions must
       actually be present at exactly ``tid``.
    """
    if tid is None:
        return None
    if not isinstance(tid, (bytes, bytearray)):
        # A minimal storage stub may pass a non-tid; the real facts
        # (lastTransaction/loadSerial) cannot be compared to it.
        return None

    try:
        in_flight = storage.tpc_transaction()
    except AttributeError:
        in_flight = None
    except Exception:
        # The storage cannot answer; do not guess from this fact.
        in_flight = None
    else:
        if in_flight is not None:
            return False

    try:
        last_tid = storage.lastTransaction()
    except Exception:
        return None

    if _tid_greater(tid, last_tid):
        return False
    if last_tid == tid:
        return True

    # A later transaction exists.  Verify that ours really landed.
    checked = False
    for oid in oids:
        try:
            storage.loadSerial(oid, tid)
        except POSException.POSKeyError:
            checked = True
            continue
        except AttributeError:
            # Storage cannot produce historical revisions; it also
            # cannot prove the revision missing, so stay undecided.
            return None
        except POSException.Unsupported:
            return None
        except Exception:
            # Storage unreachable after an uncertain finish: do not
            # guess "aborted" and risk rolling back published data.
            return None
        else:
            return True
    return False if checked else None


class Base:

    _copy_methods = (
        'getName', 'getSize', 'history', 'lastTransaction', 'sortKey',
        'loadBlob', 'openCommittedBlobFile',
        'isReadOnly', 'supportsUndo', 'undoLog', 'undoInfo',
        'temporaryDirectory',
    )

    def __init__(self, storage):
        self._storage = storage
        if interfaces.IBlobStorage.providedBy(storage):
            zope.interface.alsoProvides(self, interfaces.IBlobStorage)

    def __getattr__(self, name):
        if name in self._copy_methods:
            m = getattr(self._storage, name)
            setattr(self, name, m)
            return m

        raise AttributeError(name)

    def __len__(self):
        return len(self._storage)


class MVCCAdapter(Base):

    def __init__(self, storage):
        Base.__init__(self, storage)
        self._instances = set()
        self._lock = Lock()
        # Committed-invalidation journal: tid -> frozen oid set.  Lets
        # recovery and late/reopened instances observe exactly once, in
        # tid order, every invalidation the adapter has published.
        self._published = {}
        self._last_published = b''
        # Live TransactionState objects, including detached ones whose
        # committing instance went away before the verdict.
        self._states = set()
        if hasattr(storage, 'registerDB'):
            storage.registerDB(self)

    def new_instance(self):
        instance = MVCCAdapterInstance(self)
        with self._lock:
            self._instances.add(instance)
        return instance

    def before_instance(self, before=None):
        return HistoricalStorageAdapter(self._storage, before)

    def undo_instance(self):
        return UndoAdapterInstance(self)

    def _release(self, instance):
        with self._lock:
            self._instances.discard(instance)

    def _register_state(self, state):
        with self._lock:
            self._states.add(state)

    def _unregister_state(self, state):
        with self._lock:
            self._states.discard(state)

    def _detach_state(self, state):
        with self._lock:
            state.detached = True

    def _prune_published(self):
        # Called with self._lock held.  Only forget journal entries that
        # every live instance has already observed.
        if len(self._published) <= 100:
            return
        try:
            watermark = min(
                (instance._ltid for instance in self._instances
                 if instance._ltid),
                default=b'')
        except ValueError:
            return
        stale = [tid for tid in self._published if tid < watermark]
        for tid in stale:
            del self._published[tid]

    closed = False

    def close(self):
        if not self.closed:
            self.closed = True
            self._storage.close()
            del self._instances
            del self._states
            del self._published
            del self._storage

    def invalidateCache(self):
        with self._lock:
            for instance in tuple(self._instances):
                instance._invalidateCache()

    def invalidate(self, transaction_id, oids):
        with self._lock:
            for instance in tuple(self._instances):
                instance._invalidate(transaction_id, oids)

    def _invalidate_finish(self, tid, oids, committing_instance):
        return self.publish_invalidation(tid, oids, committing_instance)

    def publish_invalidation(self, tid, oids, committing_instance=None):
        """Publish invalidations for ``tid`` exactly once, in tid order.

        Returns True if this call published them, False if they were
        already published (e.g. the finish callback fired, the answer
        was lost and recovery caught up).  Instances that joined later
        learn what they missed through :meth:`MVCCAdapterInstance.poll_invalidations`.
        """
        oids = frozenset(oids)
        with self._lock:
            if tid in self._published:
                return False
            self._published[tid] = oids
            if _tid_greater(tid, self._last_published):
                self._last_published = tid
            for instance in tuple(self._instances):
                if instance is not committing_instance:
                    instance._invalidate(tid, oids)
            self._prune_published()
        return True

    references = serialize.referencesf
    transform_record_data = untransform_record_data = lambda self, data: data

    def pack(self, pack_time, referencesf):
        return self._storage.pack(pack_time, referencesf)


class MVCCAdapterInstance(Base):

    _copy_methods = Base._copy_methods + (
        'loadSerial', 'new_oid',
        'checkCurrentSerialInTransaction',
    )

    _start = None  # Transaction start time
    _ltid = b''   # Last storage transaction id

    def __init__(self, base):
        self._base = base
        Base.__init__(self, base._storage)
        self._lock = Lock()
        self._invalidations = set()
        self._sync = getattr(self._storage, 'sync', lambda: None)
        self._txn_state = None

    def release(self):
        state = self._txn_state
        if state is not None and not state.is_settled():
            # The connection goes away before the verdict is in: the
            # base keeps the state so recovery can still publish
            # invalidations; storage facts decide the outcome.
            self._base._detach_state(state)
        self._base._release(self)

    close = release

    def _invalidateCache(self):
        with self._lock:
            self._invalidations = None

    def _invalidate(self, tid, oids):
        with self._lock:
            # Never move the watermark backwards: a recovered, older
            # transaction must not mix into the state of newer ones.
            if _tid_greater(tid, self._ltid):
                self._ltid = tid
            try:
                self._invalidations.update(oids)
            except AttributeError:
                if self._invalidations is not None:
                    raise

    def sync(self, force=True):
        if force:
            self._sync()

    def poll_invalidations(self):
        # Storage implementations don't always call invalidate() when
        # the last TID changes, e.g. after network reconnection,
        # so we still have to poll.
        ltid = self._storage.lastTransaction()
        # But at this precise moment, a transaction may be committed and
        # we have already received the new tid, along with invalidations.
        base_last_published = self._base._last_published
        with self._lock:
            # So we must pick the greatest value.
            self._start = p64(u64(max(ltid, self._ltid)) + 1)
            # A storage-side gap we never got invalidations for means
            # the storage was reopened/reconnected with transactions we
            # cannot map to oids.  Flush the whole cache rather than
            # exposing a mix of old and new state.
            external_gap = (
                _tid_greater(ltid, self._ltid)
                and _tid_greater(ltid, base_last_published)
            )
            if external_gap or self._invalidations is None:
                # special value: the transaction is so old that
                # we need to flush the whole cache.
                self._invalidations = set()
                if _tid_greater(ltid, self._ltid):
                    self._ltid = ltid
                return None
            else:
                result = list(self._invalidations)
                self._invalidations.clear()
                return result

    def load(self, oid):
        assert self._start is not None
        r = self._storage.loadBefore(oid, self._start)
        if r is None:
            # object was deleted or not-yet-created.
            # raise ReadConflictError - not - POSKeyError due to backward
            # compatibility: a pack(t+δ) could be running simultaneously to our
            # transaction that observes database as of t state. Such pack,
            # because it packs the storage from a "future-to-us" point of view,
            # can remove object revisions that we can try to load, for example:
            #
            #   txn1            <-- t
            #        obj.revA
            #
            #   txn2            <-- t+δ
            #        obj.revB
            #
            # for such case we want user transaction to be restarted - not
            # failed - by raising ConflictError subclass.
            #
            # XXX we don't detect for pack to be actually running - just assume
            # the worst. It would be good if storage could provide information
            # whether pack is/was actually running and its details, take that
            # into account, and raise ReadConflictError only in the presence of
            # database being simultaneously updated from back of its log.
            raise POSException.ReadConflictError(
                "load %s @%s: object deleted, likely by simultaneous pack" %
                (oid_repr(oid), tid_repr(p64(u64(self._start) - 1))))

        return r[:2]

    def prefetch(self, oids):
        try:
            self._storage.prefetch(oids, self._start)
        except AttributeError:
            if not hasattr(self._storage, 'prefetch'):
                self.prefetch = lambda *a: None
            else:
                raise

    _modified = None  # Used to keep track of oids modified within a
    # transaction, so we can invalidate them later.

    def _tentative_tid(self, transaction):
        """Best-effort storage-allocated tid for the live transaction."""
        storage = self._storage
        getter = getattr(storage, 'tpc_tid', None)
        if getter is not None:
            try:
                return getter(transaction)
            except Exception:
                logger.debug(
                    "tpc_tid(%r) failed; tid will be learned at finish",
                    transaction, exc_info=True)
        return getattr(storage, '_tid', None)

    def tpc_begin(self, transaction):
        self._storage.tpc_begin(transaction)
        self._modified = set()
        tid = self._tentative_tid(transaction)
        state = TransactionState(self._base, transaction, tid)
        state.modified = self._modified
        setattr(transaction, TXN_STATE_ATTR, state)
        self._txn_state = state
        self._base._register_state(state)

    def store(self, oid, serial, data, version, transaction):
        self._storage.store(oid, serial, data, version, transaction)
        self._modified.add(oid)

    def storeBlob(self, oid, serial, data, blobfilename, version, transaction):
        self._storage.storeBlob(
            oid, serial, data, blobfilename, '', transaction)
        self._modified.add(oid)

    def _publish(self, state, tid, oids):
        """Complete the invalidation step once, in tid order."""
        if tid is None:
            return
        committing = None if state.detached else self
        if self._base.publish_invalidation(tid, oids, committing):
            logger.debug(
                "Published invalidation for %s (%d oids)",
                tid_repr(tid), len(oids) if oids is not None else 0)
        with self._lock:
            if _tid_greater(tid, self._ltid):
                self._ltid = tid
        state.note_invalidated()

    def tpc_vote(self, transaction):
        state = getattr(transaction, TXN_STATE_ATTR, None)
        try:
            result = self._storage.tpc_vote(transaction)
        except BaseException as exc:
            # tpc_abort follows and cleans up connection, storage and
            # staged blobs; remember that the vote verdict is final.
            if state is not None:
                state.note_vote_failed(exc)
            raise
        if state is not None:
            state.note_voted()
        return result

    def _resolve_finish_failure(self, state, modified):
        """Storage tpc_finish raised.  Determine the verdict from facts.

        Never writes to the storage and never asks it to abort a
        transaction it may have committed.
        """
        tid = state.tid or self._tentative_tid(state.transaction)
        if tid is not None:
            state.tid = tid
        committed = storage_committed(self._storage, tid, modified)
        if committed is None:
            # Undecidable from facts: the finish callback runs at the
            # storage commit point, so a fired callback proves
            # publication.
            committed = state.callback_fired
        if committed:
            logger.critical(
                "tpc_finish failed after the storage committed %s; "
                "completing cache invalidation and blob finalization "
                "without rewriting or aborting the transaction",
                tid_repr(tid) if tid is not None else '<unknown tid>',
                exc_info=state.finish_error)
            self._publish(state, tid, modified)
            recovered = getattr(self._storage, 'tpc_recovered', None)
            if recovered is not None:
                try:
                    recovered(state.transaction, tid)
                except Exception:
                    logger.exception(
                        "Storage recovery hook failed for %s",
                        tid_repr(tid) if tid is not None else '<tid>')
            state.mark_committed()
            return tid
        state.mark_failed()
        return None

    def tpc_finish(self, transaction, func=lambda tid: None):
        state = getattr(transaction, TXN_STATE_ATTR, None)

        modified = self._modified
        self._modified = None

        if state is None:
            # Defensive: a storage instance used without our tpc_begin
            # bookkeeping keeps the historical, direct behaviour.
            return self._storage.tpc_finish(transaction, func)

        state.note_finishing()

        def invalidate_finish(tid):
            # Runs at the storage commit point, while the storage holds
            # its commit lock.
            if isinstance(tid, (bytes, bytearray)):
                state.tid = tid
            state.callback_fired = True
            self._publish(state, tid, modified)
            func(tid)

        try:
            tid = self._storage.tpc_finish(transaction, invalidate_finish)
        except BaseException as exc:
            state.finish_error = exc
            tid = self._resolve_finish_failure(state, modified)
            if tid is None:
                # Confirmed (or best-effort judged) not committed; the
                # transaction package will call tpc_abort, which performs
                # the real cleanup.  Re-raise the original error.
                raise
            return tid

        if isinstance(tid, (bytes, bytearray)):
            state.tid = tid
        # Storages are contractually required to call the callback, but
        # do not trust a broken/remote one: complete the invalidation
        # step from here if it did not.
        if not state.invalidated and state.tid is not None:
            self._publish(state, state.tid, modified)
        recovered = getattr(self._storage, 'tpc_recovered', None)
        if recovered is not None:
            try:
                recovered(transaction, tid)
            except Exception:
                logger.exception(
                    "Storage recovery hook failed for %s", tid_repr(tid))
        state.mark_committed()
        return tid

    def tpc_abort(self, transaction):
        state = getattr(transaction, TXN_STATE_ATTR, None)
        if state is not None:
            if state.phase == TransactionState.FINISHING:
                # An abort racing with a finish: wait for the verdict
                # instead of rolling back a possibly published txn.
                state.wait_settled()
            if state.is_committed():
                # The storage published this transaction; "aborting" it
                # must complete, not undo, the commit.
                if not state.invalidated and state.tid is not None:
                    self._publish(state, state.tid, state.modified)
                recovered = getattr(self._storage, 'tpc_recovered', None)
                if recovered is not None:
                    try:
                        recovered(transaction, state.tid)
                    except Exception:
                        logger.exception(
                            "Storage recovery hook failed for %s",
                            tid_repr(state.tid))
                return
        self._storage.tpc_abort(transaction)
        if state is not None:
            state.mark_aborted()


def read_only_writer(self, *a, **kw):
    raise POSException.ReadOnlyError


class HistoricalStorageAdapter(Base):
    """Adapt a storage to a historical storage
    """

    _copy_methods = Base._copy_methods + (
        'loadSerial', 'tpc_begin', 'tpc_finish', 'tpc_abort', 'tpc_vote',
        'checkCurrentSerialInTransaction',
    )

    def __init__(self, storage, before=None):
        Base.__init__(self, storage)
        self._before = before

    def isReadOnly(self):
        return True

    def supportsUndo(self):
        return False

    def release(self):
        try:
            release = self._storage.release
        except AttributeError:
            pass
        else:
            release()

    close = release

    def sync(self, force=True):
        pass

    def poll_invalidations(self):
        return []

    new_oid = pack = store = read_only_writer

    def load(self, oid, version=''):
        r = self._storage.loadBefore(oid, self._before)
        if r is None:
            raise POSException.POSKeyError(oid)
        return r[:2]


class UndoAdapterInstance(Base):

    _copy_methods = Base._copy_methods + (
        'tpc_abort',
    )

    def __init__(self, base):
        self._base = base
        Base.__init__(self, base._storage)

    def release(self):
        pass

    close = release

    def tpc_begin(self, transaction):
        self._storage.tpc_begin(transaction)
        self._undone = set()

    def undo(self, transaction_id, transaction):
        result = self._storage.undo(transaction_id, transaction)
        if result:
            self._undone.update(result[1])
        return result

    def tpc_vote(self, transaction):
        result = self._storage.tpc_vote(transaction)
        if result:
            self._undone.update(result)

    def tpc_finish(self, transaction, func=lambda tid: None):

        def invalidate_finish(tid):
            self._base.publish_invalidation(tid, self._undone, None)
            func(tid)

        self._storage.tpc_finish(transaction, invalidate_finish)
