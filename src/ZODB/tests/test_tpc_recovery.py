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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Recovery tests for an uncertain tpc_finish.

These tests pin down the rule that one two-phase commit has exactly one
conclusion: the storage-allocated tid, the object data, the blob files,
the connection's created objects and the MVCC invalidations are all
bound to the same verdict.  A tpc_finish that fails *after* the storage
committed is completed from storage facts -- the transaction is never
written twice and a published revision is never rolled back.
"""
import os
import shutil
import tempfile
import threading
import unittest

import transaction
import zope.interface
from persistent.mapping import PersistentMapping

from ZODB import DB
from ZODB.blob import Blob
from ZODB.blob import BlobStorage
from ZODB.Connection import TransactionMetaData
from ZODB.FileStorage import FileStorage
from ZODB.interfaces import IBlobStorage
from ZODB.MappingStorage import MappingStorage
from ZODB.mvccadapter import MVCCAdapter
from ZODB.mvccadapter import TransactionState
from ZODB.mvccadapter import storage_committed
from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.utils import p64
from ZODB.utils import tid_repr
from ZODB.utils import u64
from ZODB.utils import z64


class LostResponseError(Exception):
    """Stand-in for a lost storage response (network reset, ...)."""


class UncertainFinishStorage(MappingStorage):
    """tpc_finish commits, then the response is lost.

    The data really lands in the storage and the callback may already
    have run; nevertheless tpc_finish raises, exactly like a remote
    storage whose answer was lost after commit.
    """

    deliver_callback = True

    def __init__(self, deliver_callback=True, *args, **kw):
        super().__init__(*args, **kw)
        self.deliver_callback = deliver_callback
        self.abort_calls = 0
        self.extra_finish_calls = 0
        self.store_calls = 0

    def store(self, *a, **kw):
        self.store_calls += 1
        return super().store(*a, **kw)

    def tpc_abort(self, transaction):
        self.abort_calls += 1
        return super().tpc_abort(transaction)

    def tpc_finish(self, transaction, func=lambda tid: None):
        if self.deliver_callback:
            tid = super().tpc_finish(transaction, func)
        else:
            # The storage commits but never invokes the callback
            # (response lost before the invalidation message arrived).
            tid = super().tpc_finish(transaction, lambda t: None)
        raise LostResponseError("response lost after commit of %s" %
                                tid_repr(tid))


class CallbackFailsBeforeCommitStorage(MappingStorage):
    """The finish callback fails before the storage publishes data.

    MappingStorage publishes only after the callback returned, so the
    storage facts say *not committed*; the transaction must abort.
    Failure is switchable so database construction is unaffected.
    """

    fail_finish = False

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.abort_calls = 0

    def tpc_abort(self, transaction):
        self.abort_calls += 1
        return super().tpc_abort(transaction)

    def tpc_finish(self, transaction, func=lambda tid: None):
        if not self.fail_finish:
            return super().tpc_finish(transaction, func)

        def cb(tid):
            raise LostResponseError("callback failed")
        return super().tpc_finish(transaction, cb)


@zope.interface.implementer(IBlobStorage)
class VoteFailingBlobStorage(BlobStorage):

    fail_vote = False

    def tpc_vote(self, transaction):
        if self.fail_vote:
            raise ConflictError("vote says no")
        # BlobStorage itself has no tpc_vote; the proxied storage does.
        vote = super().__getattr__('tpc_vote')
        return vote(transaction)


@zope.interface.implementer(IBlobStorage)
class UncertainBlobStorage(BlobStorage):

    def tpc_finish(self, *arg, **kw):
        super().tpc_finish(*arg, **kw)
        raise LostResponseError("response lost after blob commit")


class PostCommitCallbackFailsStorage(MappingStorage):
    """Data are published, the finish callback runs, then the answer is
    lost.  Recovery must complete the committed transaction without
    re-running the callback or writing again."""

    def tpc_finish(self, transaction, func=lambda tid: None):
        # Publish first (no callback), then invoke the callback at the
        # commit point and fail like a storage whose tail response died.
        tid = MappingStorage.tpc_finish(
            self, transaction, lambda t: None)
        func(tid)
        raise LostResponseError("tail failed after commit point")


class StorageFactsTests(unittest.TestCase):

    def test_facts_no_tid_is_undecidable(self):
        storage = MappingStorage()
        self.assertIsNone(storage_committed(storage, None, ()))

    def test_facts_not_committed_before_publish(self):
        storage = MappingStorage()
        txn = TransactionMetaData()
        storage.tpc_begin(txn)
        tid = storage._tid
        # Still in flight and behind lastTransaction: clearly aborted.
        self.assertIs(storage_committed(storage, tid, ()), False)
        storage.tpc_abort(txn)
        # Afterwards lastTransaction is still older: not committed.
        self.assertIs(storage_committed(storage, tid, ()), False)

    def test_facts_committed_when_last_tid_matches(self):
        storage = MappingStorage()
        txn = TransactionMetaData()
        storage.tpc_begin(txn)
        oid = p64(1)
        storage.store(oid, z64, b'pickle', '', txn)
        storage.tpc_vote(txn)
        tid = storage.tpc_finish(txn)
        self.assertIs(storage_committed(storage, tid, (oid,)), True)

    def test_facts_later_transaction_distinguished_by_revision(self):
        storage = MappingStorage()
        txn1 = TransactionMetaData()
        storage.tpc_begin(txn1)
        oid = p64(1)
        storage.store(oid, z64, b'one', '', txn1)
        storage.tpc_vote(txn1)
        tid1 = storage.tpc_finish(txn1)

        # A hypothetical tentative tid strictly between committed tids
        # must not be mistaken for committed, even though lastTransaction
        # is larger.
        tentative = p64(u64(tid1) + 1)
        self.assertIs(storage_committed(storage, tentative, (oid,)), False)

        txn2 = TransactionMetaData()
        storage.tpc_begin(txn2)
        storage.store(oid, tid1, b'two', '', txn2)
        storage.tpc_vote(txn2)
        tid2 = storage.tpc_finish(txn2)
        # tid1 is still verifiable, even with a later txn on top.
        self.assertIs(storage_committed(storage, tid1, (oid,)), True)
        self.assertTrue(u64(tid2) > u64(tid1))


class MVCCAdapterRecoveryTests(unittest.TestCase):

    def _adapter(self, storage):
        mvcc = MVCCAdapter(storage)
        return mvcc, mvcc.new_instance()

    def _commit_one(self, instance, oid=p64(1), data=b'pickle'):
        txn = TransactionMetaData()
        instance.tpc_begin(txn)
        instance.store(oid, z64, data, '', txn)
        instance.tpc_vote(txn)
        return txn

    def test_finish_lost_after_callback_is_completed(self):
        storage = UncertainFinishStorage(deliver_callback=True)
        mvcc, instance = self._adapter(storage)
        txn = self._commit_one(instance)
        tid = instance.tpc_finish(txn)
        self.assertEqual(tid, storage.lastTransaction())
        # No second write, no abort of the published revision.
        self.assertEqual(storage.abort_calls, 0)
        state = getattr(txn, '_zodb_txn_state')
        self.assertEqual(state.phase, TransactionState.COMMITTED)
        self.assertTrue(state.invalidated)
        self.assertIn(tid, mvcc._published)
        # Data really readable at the committed tid.
        self.assertEqual(instance._storage.loadSerial(p64(1), tid), b'pickle')

    def test_finish_lost_without_callback_is_completed(self):
        storage = UncertainFinishStorage(deliver_callback=False)
        mvcc, instance = self._adapter(storage)
        other = mvcc.new_instance()
        txn = self._commit_one(instance)
        # Pre-allocated tid survives the missing callback.
        self.assertEqual(getattr(txn, '_zodb_txn_state').tid, storage._tid)
        tid = instance.tpc_finish(txn)
        self.assertEqual(tid, storage.lastTransaction())
        self.assertEqual(storage.abort_calls, 0)
        # The other instance still received the invalidation, exactly once.
        self.assertEqual(other._invalidations, {p64(1)})
        self.assertEqual(mvcc._published[tid], frozenset({p64(1)}))

    def test_finish_callback_failure_before_commit_aborts(self):
        storage = CallbackFailsBeforeCommitStorage()
        mvcc, instance = self._adapter(storage)
        storage.fail_finish = True
        txn = self._commit_one(instance)
        with self.assertRaises(LostResponseError):
            instance.tpc_finish(txn)
        state = getattr(txn, '_zodb_txn_state')
        self.assertEqual(state.phase, TransactionState.FAILED)
        # Transaction package follows with tpc_abort; real cleanup runs.
        instance.tpc_abort(txn)
        self.assertEqual(storage.abort_calls, 1)
        self.assertNotIn(storage._tid, mvcc._published)
        storage.fail_finish = False
        # Commit lock released: another transaction can begin and commit.
        txn2 = self._commit_one(instance, data=b'again')
        tid2 = instance.tpc_finish(txn2)
        self.assertEqual(tid2, storage.lastTransaction())

    def test_tpc_abort_after_committed_is_completion_only(self):
        storage = UncertainFinishStorage(deliver_callback=True)
        mvcc, instance = self._adapter(storage)
        txn = self._commit_one(instance)
        tid = instance.tpc_finish(txn)
        # A late/duplicate tpc_abort (e.g. multi-resource cleanup) must
        # not touch the published transaction.
        instance.tpc_abort(txn)
        self.assertEqual(storage.abort_calls, 0)
        self.assertEqual(instance._storage.loadSerial(p64(1), tid),
                         b'pickle')
        self.assertEqual(getattr(txn, '_zodb_txn_state').phase,
                         TransactionState.COMMITTED)

    def test_abort_concurrent_with_finish_keeps_committed_verdict(self):
        storage = MappingStorage()
        storage.spy_aborts = 0
        real_abort = storage.tpc_abort

        def counting_abort(transaction):
            storage.spy_aborts += 1
            return real_abort(transaction)

        mvcc, instance = self._adapter(storage)
        txn = self._commit_one(instance)
        state = getattr(txn, '_zodb_txn_state')

        finish_entered = threading.Event()
        release_finish = threading.Event()
        real_finish = storage.tpc_finish

        def slow_finish(transaction, func=lambda tid: None):
            finish_entered.set()
            self.assertTrue(release_finish.wait(10))
            return real_finish(transaction, func)

        storage.tpc_finish = slow_finish
        storage.tpc_abort = counting_abort
        result = {}

        def finisher():
            try:
                result['tid'] = instance.tpc_finish(txn)
            except BaseException as exc:  # pragma: no cover
                result['error'] = exc

        def aborter():
            self.assertTrue(finish_entered.wait(10))
            # Abort while the verdict is still pending: wait for the
            # finish instead of rolling back a possibly published txn.
            instance.tpc_abort(txn)
            result['abort_returned'] = True

        t1 = threading.Thread(target=finisher)
        t2 = threading.Thread(target=aborter)
        t1.start()
        t2.start()
        release_finish.set()
        t1.join(10)
        t2.join(10)
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())
        self.assertEqual(result.get('tid'), storage.lastTransaction())
        self.assertTrue(result.get('abort_returned'))
        # The committed transaction was never sent to storage abort.
        self.assertEqual(storage.spy_aborts, 0)
        self.assertEqual(state.phase, TransactionState.COMMITTED)
        self.assertEqual(instance._storage.loadSerial(p64(1), result['tid']),
                         b'pickle')

    def test_vote_failure_marks_state_and_aborts(self):
        storage = MappingStorage()
        mvcc, instance = self._adapter(storage)
        txn = TransactionMetaData()
        instance.tpc_begin(txn)
        instance.store(p64(1), z64, b'x', '', txn)

        original_vote = storage.tpc_vote

        def reject(transaction):
            raise ConflictError("vote says no")

        storage.tpc_vote = reject
        with self.assertRaises(ConflictError):
            instance.tpc_vote(txn)
        state = getattr(txn, '_zodb_txn_state')
        self.assertEqual(state.phase, TransactionState.FAILED)
        # The following tpc_abort cleans storage and releases the lock.
        instance.tpc_abort(txn)
        self.assertNotIn(storage._tid, mvcc._published)
        storage.tpc_vote = original_vote
        # A subsequent transaction can begin, vote and commit normally.
        txn2 = self._commit_one(instance, data=b'again')
        tid2 = instance.tpc_finish(txn2)
        self.assertEqual(tid2, storage.lastTransaction())
        self.assertEqual(instance._storage.loadSerial(p64(1), tid2),
                         b'again')

    def test_published_invalidation_is_ordered_and_deduplicated(self):
        storage = MappingStorage()
        mvcc = MVCCAdapter(storage)
        i2 = mvcc.new_instance()
        t1 = p64(10)
        t2 = p64(20)
        self.assertTrue(mvcc.publish_invalidation(t1, {p64(1)}))
        self.assertTrue(mvcc.publish_invalidation(t2, {p64(2)}))
        # Exactly once.
        self.assertFalse(mvcc.publish_invalidation(t1, {p64(1)}))
        self.assertEqual(mvcc._last_published, t2)
        # An older recovered tid never moves the watermark backwards.
        self.assertTrue(mvcc.publish_invalidation(p64(5), {p64(3)}))
        self.assertEqual(mvcc._last_published, t2)
        # i2 saw every invalidation once, with a monotone ltid.
        self.assertEqual(i2._invalidations, {p64(1), p64(2), p64(3)})
        self.assertEqual(i2._ltid, t2)

    def test_released_instance_does_not_lose_invalidation(self):
        storage = UncertainFinishStorage(deliver_callback=False)
        mvcc, instance = self._adapter(storage)
        other = mvcc.new_instance()
        txn = self._commit_one(instance)
        # The committing connection closes before the finish verdict.
        instance.release()
        state = getattr(txn, '_zodb_txn_state')
        self.assertTrue(state.detached)
        self.assertIn(state, mvcc._states)
        tid = storage._tid
        mvcc.publish_invalidation(tid, {p64(1)})
        state.mark_committed()
        self.assertEqual(other._invalidations, {p64(1)})
        self.assertNotIn(state, mvcc._states)

    def test_callback_tail_failure_after_commit_completed_once(self):
        storage = PostCommitCallbackFailsStorage()
        storage.abort_calls = 0
        real_abort = storage.tpc_abort

        def counting_abort(transaction):
            storage.abort_calls += 1
            return real_abort(transaction)

        mvcc, instance = self._adapter(storage)
        storage.tpc_abort = counting_abort
        other = mvcc.new_instance()
        txn = self._commit_one(instance)
        calls = []
        tid = instance.tpc_finish(
            txn, lambda committed_tid: calls.append(committed_tid))
        # Committed verdict; caller callback ran exactly once, never replayed.
        self.assertEqual(len(calls), 1)
        self.assertEqual(tid, calls[0])
        self.assertEqual(tid, storage.lastTransaction())
        self.assertEqual(storage.abort_calls, 0)
        self.assertIn(tid, mvcc._published)
        self.assertEqual(other._invalidations, {p64(1)})

    def test_poll_reconciles_unknown_storage_gap(self):
        storage = MappingStorage()
        mvcc, instance = self._adapter(storage)
        instance.sync()
        instance.poll_invalidations()
        # Storage "reopens" with a transaction never invalidated here.
        txn = TransactionMetaData()
        storage.tpc_begin(txn)
        storage.store(p64(7), z64, b'x', '', txn)
        storage.tpc_vote(txn)
        storage.tpc_finish(txn)
        # No call to mvcc.invalidate happened: the cache must be flushed.
        self.assertIsNone(instance.poll_invalidations())


class ConnectionRecoveryTests(unittest.TestCase):

    def setUp(self):
        self.storage = UncertainFinishStorage(deliver_callback=True)
        self.db = DB(self.storage)

    def tearDown(self):
        self.db.close()

    def test_commit_succeeds_despite_lost_finish_response(self):
        conn = self.db.open()
        other = self.db.open()
        other.root()  # populate caches before the commit
        conn.root()['answer'] = 42
        transaction.commit()
        # Caller sees success.
        self.assertEqual(conn.root()['answer'], 42)
        # The object's serial is the committed tid.
        self.assertEqual(conn.get(z64)._p_serial,
                         self.storage.lastTransaction())
        # Other connections received the invalidation exactly once.
        self.assertEqual(other.root()['answer'], 42)
        # Storage was not asked to abort/rewrite the transaction.
        self.assertEqual(self.storage.abort_calls, 0)
        conn.close()
        other.close()

    def test_persisted_after_reopen(self):
        conn = self.db.open()
        conn.root()['answer'] = 42
        transaction.commit()
        conn.close()
        fresh = self.db.open()
        self.assertEqual(fresh.root()['answer'], 42)
        fresh.close()


class ConnectionNoCallbackRecoveryTests(ConnectionRecoveryTests):

    def setUp(self):
        self.storage = UncertainFinishStorage(deliver_callback=False)
        self.db = DB(self.storage)


class ConnectionGenuineFailureTests(unittest.TestCase):

    def setUp(self):
        self.storage = CallbackFailsBeforeCommitStorage()
        self.db = DB(self.storage)

    def tearDown(self):
        self.db.close()

    def test_commit_fails_and_transaction_is_aborted(self):
        conn = self.db.open()
        ltid_before = self.storage.lastTransaction()
        conn.root()['answer'] = 42
        self.storage.fail_finish = True
        with self.assertRaises(LostResponseError):
            transaction.commit()
        self.storage.fail_finish = False
        # Nothing new was published; a subsequent commit works.
        self.assertEqual(self.storage.lastTransaction(), ltid_before)
        transaction.abort()
        conn.root()['answer'] = 43
        transaction.commit()
        self.assertEqual(conn.root()['answer'], 43)
        conn.close()


class BlobRecoveryTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.blob_dir = os.path.join(self.tmp, 'blobs')

    def tearDown(self):
        if self.db is not None:
            self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    db = None

    def _blob_path(self, storage, oid, tid):
        return storage.fshelper.getBlobFilename(oid, tid)

    def test_blob_kept_after_uncertain_committed_finish(self):
        storage = UncertainBlobStorage(
            self.blob_dir, MappingStorage())
        self.db = DB(storage)
        conn = self.db.open()
        blob = Blob()
        with blob.open('w') as f:
            f.write(b'blob data')
        conn.root()['blob'] = blob
        transaction.commit()

        obj = conn.root()['blob']
        oid = obj._p_oid
        # Activating the blob binds the committed serial (new blobs are
        # invalidated to ghosts while they are stored).
        with obj.open('r') as f:
            self.assertEqual(f.read(), b'blob data')
        tid = obj._p_serial
        # The published blob file is present and readable.
        path = self._blob_path(storage, oid, tid)
        self.assertTrue(os.path.exists(path))
        with open(path, 'rb') as f:
            self.assertEqual(f.read(), b'blob data')
        # dirty bookkeeping was dropped by the recovery hook.
        self.assertEqual(storage.dirty_oids, [])
        # A fresh connection reads the committed blob.
        other = self.db.open()
        with other.root()['blob'].open('r') as f:
            self.assertEqual(f.read(), b'blob data')
        other.close()
        conn.close()

    def test_blob_removed_after_vote_failure(self):
        storage = VoteFailingBlobStorage(
            self.blob_dir, MappingStorage())
        self.db = DB(storage)
        storage.fail_vote = True
        conn = self.db.open()

        blob = Blob()
        with blob.open('w') as f:
            f.write(b'blob data')
        conn.root()['blob'] = blob
        with self.assertRaises(ConflictError):
            transaction.commit()
        transaction.abort()

        # Every staged blob file was reclaimed; no committed path left.
        for oid, oid_path in storage.fshelper.listOIDs():
            self.assertEqual(os.listdir(oid_path), [])
        conn.close()

    def test_blob_storage_wrappers_share_tid_with_data(self):
        storage = BlobStorage(self.blob_dir, MappingStorage())
        self.db = DB(storage)
        conn = self.db.open()
        blob = Blob()
        with blob.open('w') as f:
            f.write(b'xyz')
        conn.root()['blob'] = blob
        transaction.commit()
        obj = conn.root()['blob']
        # Touching the blob activates its record with the committed serial.
        with obj.open('r') as f:
            self.assertEqual(f.read(), b'xyz')
        # Object serial, blob path revision and storage tid agree.
        self.assertEqual(obj._p_serial, storage.lastTransaction())
        self.assertTrue(os.path.exists(
            self._blob_path(storage, obj._p_oid, obj._p_serial)))
        conn.close()


class FileStorageBlobReopenTests(unittest.TestCase):
    """A crashed final phase must not leave a visible half-commit."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.fs_path = os.path.join(self.tmp, 'data.fs')
        self.blob_dir = os.path.join(self.tmp, 'blobs')

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_reopen_removes_blob_of_truncated_transaction(self):
        # Commit a valid blob transaction first.
        storage = BlobStorage(self.blob_dir,
                              FileStorage(self.fs_path, create=True))
        db = DB(storage)
        conn = db.open()
        good = Blob()
        with good.open('w') as f:
            f.write(b'good')
        conn.root()['good'] = good
        transaction.commit()
        good_oid = conn.root()['good']._p_oid
        good_tid = storage.lastTransaction()
        db.close()

        # Simulate a crash between vote and finish: the checkpoint
        # transaction (status 'c') and a staged blob file are on disk,
        # but tpc_finish never ran.
        fs = FileStorage(self.fs_path)
        bs = BlobStorage(self.blob_dir, fs)
        txn = TransactionMetaData()
        bs.tpc_begin(txn)
        crashed_tid = fs._tid
        crashed_oid = p64(900)
        staged = os.path.join(self.tmp, 'incoming')
        with open(staged, 'wb') as f:
            f.write(b'crashed')
        bs.storeBlob(crashed_oid, z64, b'pickle', staged, '', txn)
        bs.tpc_vote(txn)
        staged_path = bs.fshelper.getBlobFilename(crashed_oid, crashed_tid)
        self.assertTrue(os.path.exists(staged_path))
        # Hard close without finish/abort, like a killed process.
        fs.close()

        # Reopen: FileStorage truncates the trailing 'c' transaction.
        fs2 = FileStorage(self.fs_path)
        self.assertTrue(fs2._recovered_incomplete)
        # The truncated transaction's data is gone from the storage.
        with self.assertRaises(POSKeyError):
            fs2.loadSerial(crashed_oid, crashed_tid)
        # ... while the last committed revision is intact.
        self.assertTrue(fs2.loadSerial(good_oid, good_tid))
        self.assertGreaterEqual(fs2.lastTransaction(), good_tid)
        # Blob reconciliation removes only the orphan blob.
        bs2 = BlobStorage(self.blob_dir, fs2)
        self.assertFalse(os.path.exists(staged_path))
        self.assertTrue(os.path.exists(
            bs2.fshelper.getBlobFilename(good_oid, good_tid)))
        with bs2.openCommittedBlobFile(good_oid, good_tid) as f:
            self.assertEqual(f.read(), b'good')
        fs2.close()

    def test_clean_reopen_does_not_reconcile(self):
        storage = BlobStorage(self.blob_dir,
                              FileStorage(self.fs_path, create=True))
        db = DB(storage)
        conn = db.open()
        conn.root()['x'] = 1
        transaction.commit()
        db.close()

        fs = FileStorage(self.fs_path)
        self.assertFalse(fs._recovered_incomplete)
        fs.close()


class CompatibilityTests(unittest.TestCase):

    def setUp(self):
        self.db = DB(MappingStorage())

    def tearDown(self):
        self.db.close()

    def test_readonly_historical_connection(self):
        conn = self.db.open()
        conn.root()['v'] = 1
        transaction.commit()
        tid = self.db.lastTransaction()
        hist = self.db.open(before=tid)
        self.assertNotIn('v', hist.root())
        hist.close()
        conn.close()

    def test_empty_commit(self):
        conn = self.db.open()
        transaction.commit()
        transaction.commit()
        conn.close()

    def test_savepoint_rollback_and_commit(self):
        conn = self.db.open()
        conn.root()['keep'] = PersistentMapping()
        transaction.commit()

        sp = transaction.savepoint()
        conn.root()['keep']['temp'] = 1
        sp.rollback()
        transaction.commit()
        self.assertNotIn('temp', self.db.open().root()['keep'])

        conn.root()['keep']['final'] = 2
        sp = transaction.savepoint()  # noqa: F841
        transaction.commit()
        self.assertEqual(self.db.open().root()['keep']['final'], 2)
        conn.close()


def test_suite():
    return unittest.defaultTestLoader.loadTestsFromName(__name__)
