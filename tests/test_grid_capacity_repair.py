import copy
import importlib.util
from pathlib import Path
import unittest
from unittest import mock
import os
import tempfile
import signal
import tarfile


spec = importlib.util.spec_from_file_location(
    'grid_capacity_repair', Path(__file__).resolve().parents[1] / 'scripts' / 'repair-anthropic-capacity.py'
)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


class GridCapacityRepairTests(unittest.TestCase):
    def fixture(self):
        shape = {'symbol': repair.SYMBOL, 'side': 'Sell', 'quantity': '0.40', 'price': '1932.00000',
                 'reduce_only': False, 'kind': 'Limit', 'time_in_force': 'Gtc'}
        source = {'exchange_order_id': str(repair.ORDER), 'terminal_status': None,
                  'purpose': {'kind': 'replacement', 'level_index': 5, 'obligation_ids': [73]},
                  'cumulative_quantity': '0', 'shape': shape, 'execution_audit': None}
        state = {'run_id': repair.RUN, 'lifecycle': 'running', 'plan': {}, 'config': {
            'grid_count': 20, 'grid_order_qty': '0.2', 'position_sizing_mode': 'fixed_grid_qty'},
            'orders': {repair.CLIENT: source}, 'replacement_obligations': {},
            'next_order_sequence': 106, 'next_obligation_sequence': 100, 'revision': 339,
            'baseline': {'signed_quantity': '-0.5', 'entry_price': '1948.88'},
            'grid_position_net_quantity': '0', 'inventory_events': {'history': 'immutable'}}
        ledger = {'intents': {repair.CLIENT: {'state': {'kind': 'accepted'}, 'shape': copy.deepcopy(shape)}},
                  'cancellations': {}, 'revision': 100}
        receipt = {'symbol': repair.SYMBOL, 'clientOrderId': repair.CLIENT, 'orderId': repair.ORDER,
                   'status': 'CANCELED', 'side': 'SELL', 'reduceOnly': False, 'type': 'LIMIT',
                   'timeInForce': 'GTC', 'price': '1932', 'origQty': '.4', 'executedQty': '0',
                   'time': 10, 'updateTime': 20}
        return {'runtime_state': 'active', 'state': state}, ledger, receipt

    def test_repair_preserves_manual_baseline_and_history_and_retires_only_point_two(self):
        wrapper, ledger, receipt = self.fixture()
        before = copy.deepcopy((wrapper, ledger))
        updated, updated_ledger, target = repair.candidate_ledgers(wrapper, ledger, receipt, 30)
        self.assertEqual((wrapper, ledger), before)
        state = updated['state']
        self.assertEqual(state['baseline'], wrapper['state']['baseline'])
        self.assertEqual(state['inventory_events'], wrapper['state']['inventory_events'])
        self.assertEqual(state['orders'][repair.CLIENT]['shape']['quantity'], '0.40')
        self.assertEqual(state['orders'][target]['shape']['quantity'], '0.2')
        self.assertEqual(state['replacement_obligations']['100']['shape']['quantity'], '0.40')
        self.assertEqual(state['replacement_obligations']['100']['retired_quantity'], '0.2')
        self.assertEqual(state['version'], 2)
        self.assertEqual(updated_ledger['intents'][repair.CLIENT]['shape'], ledger['intents'][repair.CLIENT]['shape'])
        self.assertNotIn(target, updated_ledger['intents'])
        self.assertEqual(state['orders'][target]['tracking'], {'kind': 'ready'})

    def test_fill_race_wrong_identity_or_incomplete_cancel_can_never_create_replacement(self):
        for field, value in [('executedQty', '.01'), ('orderId', 123), ('clientOrderId', 'manual'),
                             ('side', 'BUY'), ('status', 'NEW'), ('reduceOnly', True),
                             ('origQty', '.5'), ('price', '1934')]:
            with self.subTest(field=field):
                wrapper, ledger, receipt = self.fixture()
                receipt[field] = value
                with self.assertRaises(RuntimeError):
                    repair.candidate_ledgers(wrapper, ledger, receipt, 30)

    def test_reapplying_the_same_retirement_is_rejected_not_submitted_twice(self):
        wrapper, ledger, receipt = self.fixture()
        updated, updated_ledger, _ = repair.candidate_ledgers(wrapper, ledger, receipt, 30)
        with self.assertRaises(RuntimeError):
            repair.candidate_ledgers(updated, updated_ledger, receipt, 40)

    @unittest.skipUnless(os.name == 'posix', 'production repair uses Linux ownership and directory fsync')
    def test_atomic_replacement_preserves_existing_owner_and_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'intents.json'
            path.write_text('{}')
            path.chmod(0o640)
            before = path.stat()
            repair.atomic_json(path, {'updated': True})
            after = path.stat()
            self.assertEqual((after.st_uid, after.st_gid, after.st_mode), (before.st_uid, before.st_gid, before.st_mode))

    def test_post_deploy_failure_stops_runtime_without_any_exchange_write(self):
        repair.REPAIR_IN_PROGRESS = True
        try:
            with mock.patch.object(repair, 'main', side_effect=RuntimeError('verification failed')):
                with mock.patch.object(repair.subprocess, 'run') as run:
                    with self.assertRaises(RuntimeError):
                        repair.guarded_main()
                    run.assert_called_once_with(['docker', 'stop', '--time', '15', repair.CONTAINER], check=False, timeout=30)
        finally:
            repair.REPAIR_IN_PROGRESS = False

    def test_guards_remain_active_under_python_optimization(self):
        import subprocess
        import sys
        code = ('import importlib.util; '
                f's=importlib.util.spec_from_file_location("repair", {str(Path(repair.__file__))!r}); '
                'm=importlib.util.module_from_spec(s); s.loader.exec_module(m); m.require(False, "blocked")')
        result = subprocess.run([sys.executable, '-O', '-c', code], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('RuntimeError: blocked', result.stderr)

    def test_termination_and_hangup_signals_quiesce_runtime_and_restore_handlers(self):
        for name in ('SIGTERM', 'SIGHUP'):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            original = signal.getsignal(signum)

            def interrupted_main():
                handler = signal.getsignal(signum)
                self.assertTrue(callable(handler))
                handler(signum, None)

            repair.REPAIR_IN_PROGRESS = True
            try:
                with mock.patch.object(repair, 'main', side_effect=interrupted_main):
                    with mock.patch.object(repair.subprocess, 'run') as run:
                        with self.assertRaisesRegex(RuntimeError, 'signal'):
                            repair.guarded_main()
                        run.assert_called_once_with(['docker', 'stop', '--time', '15', repair.CONTAINER], check=False, timeout=30)
                self.assertEqual(signal.getsignal(signum), original)
            finally:
                repair.REPAIR_IN_PROGRESS = False
                signal.signal(signum, original)

    @unittest.skipUnless(os.name == 'posix', 'production archive durability requires Linux directory fsync')
    def test_backup_is_readable_and_sync_failure_prevents_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / 'data'
            data.mkdir()
            (data / 'strategy.json').write_text('{"history":"preserved"}')
            backup = root / 'backup'
            backup.mkdir()
            with mock.patch.object(repair, 'DATA', data):
                repair.durable_backup(backup)
                with tarfile.open(backup / 'data-before.tar.gz') as archive:
                    self.assertEqual(archive.extractfile('data-rust-production/strategy.json').read(), b'{"history":"preserved"}')
                with mock.patch.object(repair.os, 'fsync', side_effect=OSError('sync failed')):
                    with self.assertRaisesRegex(OSError, 'sync failed'):
                        repair.durable_backup(backup)


if __name__ == '__main__':
    unittest.main()
