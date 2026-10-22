from allways.validator.relay.ledger import RelayLedger
from allways.validator.store.db import Store
from allways.validator.world import persist


def test_a_changed_table_shape_is_recreated_not_migrated(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    assert store.declare('x', 1, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER)')])
    with store.tx() as c:
        c.execute('INSERT INTO t VALUES (1)')
    assert not store.declare('x', 1, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER)')])  # same shape: kept
    assert store.conn().execute('SELECT COUNT(*) FROM t').fetchone()[0] == 1
    assert store.declare('x', 2, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER, b TEXT)')])  # new shape: fresh
    assert store.conn().execute('SELECT COUNT(*) FROM t').fetchone()[0] == 0


def test_a_failed_transaction_writes_nothing(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    try:
        with store.tx() as c:
            Store.set_meta(c, 'k', 'v')
            raise RuntimeError
    except RuntimeError:
        pass
    assert store.get_meta('k') is None


def test_the_feed_resumes_from_the_oldest_consumer_and_from_the_horizon_if_one_never_acked(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    with store.tx() as c:
        Store.ack(c, 'world', 200, 'sig-200')
        Store.ack(c, 'scoring', 150, 'sig-150')
    assert store.resume_point(['world', 'scoring']) == 'sig-150'
    assert store.resume_point(['world', 'scoring', 'relay-tao']) is None


def test_recreated_tables_drop_their_consumers_ack_so_the_feed_replays_history(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    store.declare('x', 1, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER)')])
    with store.tx() as c:
        Store.ack(c, 'x', 5, 'sig5')
        Store.ack(c, 'y', 6, 'sig6')
    store.declare('x', 1, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER)')])
    assert store.resume_point(['x', 'y']) == 'sig5'  # same shape: the position stands
    store.declare('x', 2, [('t', 'CREATE TABLE IF NOT EXISTS t (a INTEGER, b TEXT)')])
    assert store.resume_point(['x', 'y']) is None  # from the horizon
    assert store.resume_point(['y']) == 'sig6'


def test_a_recreated_relay_ledger_drops_the_relays_ack(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    RelayLedger(store, 'tao')
    with store.tx() as c:
        Store.ack(c, 'relay-tao', 5, 'sig5')
        c.execute("UPDATE shapes SET version = 0 WHERE owner = 'relay_tao'")  # an older table shape
    RelayLedger(store, 'tao')
    assert store.resume_point(['relay-tao']) is None


def test_a_world_table_bump_replays_the_feed_from_the_horizon(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    persist.declare(store)
    with store.tx() as c:
        Store.ack(c, 'world', 5, 'sig5')
        Store.ack(c, 'scoring', 5, 'sig5')
        c.execute("UPDATE shapes SET version = ? WHERE owner = 'world'", (persist.VERSION - 1,))
    persist.declare(store)
    assert store.resume_point(['world', 'scoring']) is None
