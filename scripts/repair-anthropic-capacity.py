#!/usr/bin/env python3
"""Narrow, operator-authorized repair for the 2026-10-16 legacy ANTHROPIC grid.

Default is read-only. --execute requires a verified immutable image and pauses
the runtime while changing the two durable ledgers. The only exchange write
made here is cancellation of one exact, unfilled bot-owned order. The runtime
submits the retained 0.2 using its normal validated GTC path.
"""
import argparse
import base64
import collections
import copy
import datetime
from decimal import Decimal as D
import hashlib
import hmac
import json
import os
import signal
import stat
from pathlib import Path
import subprocess
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path('/opt/grid-trading-rust-preview')
DATA = ROOT / 'data-rust-production'
RUN = 'dab0d666a310'
CLIENT = 'r_dab0d666a310_5_S_80'
ORDER = 235064031
SYMBOL = 'ANTHROPICUSDT'
CONTAINER = 'grid-trading-rust'
REPAIR_IN_PROGRESS = False


def require(condition, message='Repair precondition failed'):
    if not condition:
        raise RuntimeError(message)


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def atomic_json(path, value):
    previous = path.stat() if path.exists() else None
    temporary = path.with_name(path.name + '.capacity-repair-tmp')
    with open(temporary, 'w', encoding='utf-8') as stream:
        if previous:
            if hasattr(os, 'fchown'):
                os.fchown(stream.fileno(), previous.st_uid, previous.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(previous.st_mode))
        json.dump(value, stream, separators=(',', ':'))
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def durable_backup(backup):
    archive = backup / 'data-before.tar.gz'
    with tarfile.open(archive, 'w:gz', compresslevel=1) as tar:
        tar.add(DATA, arcname='data-rust-production')
    with open(archive, 'rb') as stream:
        os.fsync(stream.fileno())
    for path in (backup, backup.parent):
        directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def validate_receipt(receipt, status):
    require(receipt['symbol'] == SYMBOL and receipt['clientOrderId'] == CLIENT, 'Wrong symbol or owner')
    require(receipt['orderId'] == ORDER and receipt['status'] == status, 'Wrong order or cancellation state')
    require(receipt['side'] == 'SELL' and not receipt['reduceOnly'], 'Wrong order purpose')
    require(receipt['type'] == 'LIMIT' and receipt['timeInForce'] == 'GTC', 'Wrong order kind')
    require(D(receipt['price']) == D('1932') and D(receipt['origQty']) == D('.4'), 'Order shape changed')
    require(D(receipt['executedQty']) == 0, 'A fill race forbids retiring unexecuted capacity')


def candidate_ledgers(wrapper, ledger, receipt, now_ms):
    validate_receipt(receipt, 'CANCELED')
    wrapper, ledger = copy.deepcopy(wrapper), copy.deepcopy(ledger)
    state = wrapper['state']
    require(wrapper['runtime_state'] == 'active' and state['run_id'] == RUN, 'Wrong strategy')
    require(state['lifecycle'] == 'running' and state['plan'].get('allocation_version', 1) == 1, 'Wrong lifecycle or plan')
    require(state['config']['grid_count'] == 20 and D(state['config']['grid_order_qty']) == D('.2'), 'Configuration changed')
    require(state['config']['position_sizing_mode'] == 'fixed_grid_qty', 'Wrong sizing mode')
    source = state['orders'][CLIENT]
    require(source['exchange_order_id'] == str(ORDER) and source['terminal_status'] is None, 'Source identity changed')
    require(source['purpose']['level_index'] == 5 and D(source['cumulative_quantity']) == 0, 'Source execution changed')
    require(D(source['shape']['quantity']) == D('.4') and source['shape']['side'] == 'Sell', 'Source shape changed')
    require(source['shape']['price'] == '1932.00000' and not source['shape']['reduce_only'], 'Source purpose changed')
    require(source['execution_audit'] is None, 'Source audit changed')
    require(not any(not ob['assigned_client_order_id'] for ob in state['replacement_obligations'].values()), 'Pending replacements exist')
    terminal = {'kind': 'terminal', 'status': 'Cancelled', 'exchange_order_id': str(ORDER)}
    source.update(tracking={'kind': 'intent', 'state': terminal}, terminal_status='Cancelled', terminal_processed=True)
    source['execution_audit'] = {
        'snapshot': {
            'order': {'client_order_id': CLIENT, 'exchange_order_id': str(ORDER), 'exchange': 'binance',
                      'shape': copy.deepcopy(source['shape']), 'lifecycle': {'Terminal': 'Cancelled'},
                      'executed_quantity': '0'},
            'cumulative_quantity': '0', 'cumulative_quote': '0', 'fees_by_asset': {}, 'trades': [],
            'order_time_ms': receipt['time'], 'update_time_ms': receipt['updateTime'],
        },
        'fee_valuations': [], 'synced_at_ms': now_ms,
    }
    sequence = state['next_order_sequence']
    oid = state['next_obligation_sequence']
    new_client = 'r_%s_5_S_%s' % (RUN, sequence)
    shape = copy.deepcopy(source['shape'])
    shape['quantity'] = '0.2'
    state['replacement_obligations'][str(oid)] = {
        'id': oid, 'kind': 'restore_cancelled_remainder', 'source_client_order_id': CLIENT,
        'level_index': 5, 'shape': copy.deepcopy(source['shape']), 'retired_quantity': '0.2',
        'created_at_ms': now_ms, 'assigned_client_order_id': new_client,
    }
    state['orders'][new_client] = {
        'client_order_id': new_client, 'shape': shape,
        'purpose': {'kind': 'replacement', 'level_index': 5, 'obligation_ids': [oid]},
        'tracking': {'kind': 'ready'}, 'exchange_order_id': None,
        'cumulative_quantity': '0', 'cumulative_quote': '0', 'cumulative_fee': '0',
        'execution_audit': None, 'terminal_status': None, 'terminal_processed': False,
        'completed_pair_counted': False,
    }
    state['next_order_sequence'] += 1
    state['next_obligation_sequence'] += 1
    state['revision'] += 1
    state['version'] = 2
    state['updated_at_ms'] = now_ms
    intent = ledger['intents'][CLIENT]
    require(intent['state']['kind'] == 'accepted' and intent['shape'] == source['shape'], 'Intent ledger differs')
    intent.update(state=terminal, updated_at_ms=now_ms)
    ledger['cancellations'][CLIENT] = {
        'client_order_id': CLIENT, 'exchange_order_id': str(ORDER), 'exchange': 'binance',
        'symbol': SYMBOL, 'state': {'kind': 'resolved', 'status': 'Cancelled'},
        'created_at_ms': now_ms, 'updated_at_ms': now_ms,
    }
    ledger['revision'] += 1
    return wrapper, ledger, new_client


class Exchange:
    def __init__(self, container):
        from cryptography.fernet import Fernet
        env = dict(value.split('=', 1) for value in container['Config']['Env'] if '=' in value)
        master = env['GRID_CONFIG_KEY']
        try:
            cipher = Fernet(master.encode())
        except ValueError:
            salt = env.get('GRID_CONFIG_SALT')
            salt = salt.encode() if salt else hashlib.sha256(b'grid-trading-api-config-v1').digest()
            cipher = Fernet(base64.urlsafe_b64encode(hashlib.pbkdf2_hmac('sha256', master.encode(), salt, 390000, 32)))
        config = json.loads((DATA / 'api_config.json').read_text())['configs']['binance']
        require(not config.get('testnet'), 'Unexpected exchange environment')
        self.key = cipher.decrypt(config['api_key'].encode()).decode()
        self.secret = cipher.decrypt(config['api_secret'].encode())

    def request(self, path, fields, method='GET'):
        require(method == 'GET' or (method == 'DELETE' and path == '/fapi/v1/order'
                                  and dict(fields) == {'symbol': SYMBOL, 'orderId': str(ORDER)}), 'Exchange write is outside authorization')
        query = urllib.parse.urlencode(list(fields) + [('timestamp', str(int(time.time() * 1000))), ('recvWindow', '5000')])
        signature = hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()
        request = urllib.request.Request('https://fapi.binance.com' + path + '?' + query + '&signature=' + signature,
                                         headers={'X-MBX-APIKEY': self.key}, method=method)
        with urllib.request.urlopen(request, timeout=12) as response:
            return json.load(response)

    def order(self):
        return self.request('/fapi/v1/order', [('symbol', SYMBOL), ('orderId', str(ORDER))])

    def orders(self):
        return self.request('/fapi/v1/openOrders', [('symbol', SYMBOL)])

    def positions(self):
        rows = self.request('/fapi/v3/positionRisk', [('symbol', SYMBOL)])
        return sorted((row['positionSide'], str(D(row['positionAmt'])), str(D(row['entryPrice'])))
                      for row in rows if row['symbol'] == SYMBOL and D(row['positionAmt']) != 0)


def order_signature(rows):
    return {row['clientOrderId']: (str(row['orderId']), row['side'], str(D(row['price'])),
                                   str(D(row['origQty'])), str(D(row['executedQty'])), row['reduceOnly']) for row in rows}


def main():
    global REPAIR_IN_PROGRESS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--expected-revision', required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    os.chdir(ROOT)
    os.umask(0o077)
    image = json.loads(command('docker', 'image', 'inspect', args.image))[0]
    require(image['Config']['Labels']['org.opencontainers.image.revision'] == args.expected_revision, 'Image revision differs')
    container = json.loads(command('docker', 'inspect', CONTAINER))[0]
    require(container['State']['Running'] and container['State']['Health']['Status'] == 'healthy', 'Runtime is not healthy')
    exchange = Exchange(container)
    path = DATA / 'rust-control/strategies' / RUN / 'strategy.json'
    ledger_path = path.with_name('intents.json')
    active = [p.parent.name for p in path.parent.parent.glob('*/strategy.json')
              if json.loads(p.read_text())['state']['lifecycle'] not in ('stopped', 'closed')]
    require(active == [RUN], 'Unexpected live strategy; refuse to pause unrelated work')
    receipt = exchange.order()
    validate_receipt(receipt, 'NEW')
    before_position = exchange.positions()
    state = json.loads(path.read_text())['state']
    require(D(state['grid_position_net_quantity']) == 0 and not state['lots_by_level'], 'Strategy inventory changed')
    require(D(state['baseline']['signed_quantity']) == 0 and not before_position, 'Position changed: no position writes permitted')
    print('PREFLIGHT', json.dumps({'run': RUN, 'order_id': ORDER, 'price': '1932', 'from_qty': '.4', 'retain_qty': '.2',
                                  'position': before_position, 'execute': args.execute}), flush=True)
    if not args.execute:
        return
    import fcntl
    lock = open('/run/lock/grid-trading-deploy.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    project = container['Config']['Labels']['com.docker.compose.project']
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    backup = ROOT / 'backups' / ('grid-capacity-' + stamp)
    backup.mkdir(mode=0o700)
    candidate = backup / 'candidate'
    candidate.mkdir(mode=0o755)
    candidate.chmod(0o755)
    # Quiescing the application does not cancel exchange orders or close positions.
    REPAIR_IN_PROGRESS = True
    command('docker', 'stop', '--time', '15', CONTAINER)
    require(not json.loads(command('docker', 'inspect', CONTAINER))[0]['State']['Running'], 'Runtime did not quiesce')
    durable_backup(backup)
    wrapper = json.loads(path.read_text())
    ledger = json.loads(ledger_path.read_text())
    before_orders = exchange.orders()
    validate_receipt(exchange.order(), 'NEW')
    require(exchange.positions() == before_position, 'Position changed before cancellation')
    require(all(D(row['executedQty']) == 0 for row in before_orders), 'Other partial executions require reconciliation')
    require(len(before_orders) == 38 and all(RUN in row['clientOrderId'] for row in before_orders), 'Open-order set changed')
    now = int(time.time() * 1000)
    planned_receipt = dict(receipt, status='CANCELED')

    def validate_candidate(cancel_receipt, timestamp):
        updated, updated_ledger, new_client = candidate_ledgers(wrapper, ledger, cancel_receipt, timestamp)
        atomic_json(candidate / 'strategy.json', updated)
        atomic_json(candidate / 'intents.json', updated_ledger)
        for file in candidate.iterdir():
            file.chmod(0o644)
        # No credentials, network or production data are mounted into this validator.
        result = command('docker', 'run', '--rm', '--network', 'none', '--read-only',
                         '--memory', '512m', '--memory-swap', '512m', '--cpus', '0.5',
                         '--pids-limit', '64', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                         '--mount', 'type=bind,src=%s,dst=/repair,readonly' % candidate,
                         args.image, 'grid-trading-server', '--validate-strategy-file', '/repair/strategy.json')
        print('VALIDATED', result, flush=True)
        return updated, updated_ledger, new_client

    validate_candidate(planned_receipt, now)
    # Persist cancellation intent before the one and only exchange write.
    journal = {'run': RUN, 'source_client': CLIENT, 'source_order_id': ORDER,
               'retired_quantity': '.2', 'before_position': before_position,
               'before_orders': order_signature(before_orders), 'old_image': container['Config']['Image'],
               'new_image': args.image, 'revision': args.expected_revision, 'phase': 'cancel_prepared'}
    atomic_json(backup / 'repair.json', journal)
    prepared_ledger = copy.deepcopy(ledger)
    prepared_ledger['cancellations'][CLIENT] = {
        'client_order_id': CLIENT, 'exchange_order_id': str(ORDER), 'exchange': 'binance', 'symbol': SYMBOL,
        'state': {'kind': 'prepared'}, 'created_at_ms': now, 'updated_at_ms': now,
    }
    prepared_ledger['revision'] += 1
    atomic_json(ledger_path, prepared_ledger)
    try:
        exchange.request('/fapi/v1/order', [('symbol', SYMBOL), ('orderId', str(ORDER))], method='DELETE')
    except Exception as error:
        print('CANCEL_RESPONSE_UNCERTAIN', type(error).__name__, flush=True)
    cancelled = exchange.order()
    atomic_json(backup / 'exchange-cancel-receipt.json', cancelled)
    validate_receipt(cancelled, 'CANCELED')
    require(exchange.positions() == before_position, 'Position changed during cancellation')
    after_cancel = exchange.orders()
    expected_other = order_signature([row for row in before_orders if row['orderId'] != ORDER])
    require(order_signature(after_cancel) == expected_other, 'Other orders changed during repair; inspect before resuming')
    now = int(time.time() * 1000)
    updated, updated_ledger, new_client = validate_candidate(cancelled, now)
    updated_ledger['revision'] = prepared_ledger['revision'] + 1
    atomic_json(ledger_path, updated_ledger)
    atomic_json(path, updated)
    journal.update(phase='ledgers_committed', replacement_client=new_client)
    atomic_json(backup / 'repair.json', journal)
    environment = os.environ.copy()
    environment.update(GRID_RUST_IMAGE=args.image, GRID_RUST_PRODUCTION_DATA=str(DATA),
                       GRID_RUST_PRODUCTION_CONTAINER=CONTAINER, GRID_RUST_PRODUCTION_PORT='8000')
    subprocess.run(['docker', 'compose', '--project-name', project, '--file', 'docker-compose.rust-production.yml',
                    'up', '--detach', '--no-build', '--force-recreate', '--no-deps', 'grid-trading-rust'], env=environment, check=True)
    for _ in range(90):
        current = json.loads(path.read_text())['state']
        live = exchange.orders()
        replacement = next((row for row in live if row['clientOrderId'] == new_client), None)
        if replacement:
            require(D(replacement['origQty']) == D('.2') and D(replacement['price']) == D('1932'), 'Replacement shape differs')
            require(replacement['side'] == 'SELL' and not replacement['reduceOnly'], 'Replacement purpose differs')
            break
        require(current['lifecycle'] == 'running', 'Runtime did not remain running')
        time.sleep(1)
    else:
        raise RuntimeError('Replacement acknowledgement not observed; no blind manual submission is allowed')
    require(order_signature([r for r in live if r['clientOrderId'] != new_client]) == expected_other, 'Other orders changed after replacement')
    totals = collections.defaultdict(D)
    for row in live:
        totals[D(row['price'])] += D(row['origQty']) - D(row['executedQty'])
    require(len(totals) == 20 and all(qty == D('.2') for qty in totals.values()), 'Per-cell capacity differs')
    require(sum(totals.values(), D(0)) == D('4'), 'Total capacity differs')
    require(exchange.positions() == before_position, 'Position changed after replacement')
    with urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=5) as response:
        health = json.load(response)
    require(health.get('ok') and health.get('runtime_ready') and health.get('active_strategies') == 1,
            'Application health or strategy registration differs')
    current_container = json.loads(command('docker', 'inspect', CONTAINER))[0]
    require(current_container['Image'] == image['Id'] and current_container['RestartCount'] == 0, 'Image or restart count differs')
    journal.update(phase='complete', completed_at_ms=int(time.time() * 1000),
                   live_order_count=len(live), total_open_quantity='4.0', manual_position_after=before_position)
    atomic_json(backup / 'repair.json', journal)
    print('COMPLETE', json.dumps({'backup': str(backup), 'image': args.image, 'run': RUN,
        'cancelled_order': ORDER, 'replacement_client': new_client,
        'live_orders': len(live), 'cells': len(totals), 'total_quantity': '4.0',
        'manual_position_before': before_position, 'manual_position_after': exchange.positions()}), flush=True)
    REPAIR_IN_PROGRESS = False


def guarded_main():
    previous = {}

    def interrupted(signum, _frame):
        raise RuntimeError('Repair interrupted by signal %s' % signum)

    try:
        for name in ('SIGTERM', 'SIGHUP', 'SIGINT'):
            signum = getattr(signal, name, None)
            if signum is not None:
                previous[signum] = signal.signal(signum, interrupted)
        main()
    except BaseException:
        if REPAIR_IN_PROGRESS:
            # Never leave an unverified writer running and never compensate by trading.
            for signum in previous:
                signal.signal(signum, signal.SIG_IGN)
            subprocess.run(['docker', 'stop', '--time', '15', CONTAINER], check=False, timeout=30)
        raise
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == '__main__':
    guarded_main()
