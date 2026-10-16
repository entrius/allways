import threading

from allways.validator_v2.store.db import Store


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


def test_each_thread_gets_its_own_connection(tmp_path):
    store = Store(str(tmp_path / 'v.db'))
    seen = []
    t = threading.Thread(target=lambda: seen.append(store.conn()))
    t.start()
    t.join()
    assert seen[0] is not store.conn()
