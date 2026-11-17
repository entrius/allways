"""SubscriptionFeed: acks route by request id and notifications by subscription id, it is live only while every
subscription is acknowledged, its renewal is make-before-break, and stop/start opens a new generation."""

import pytest

from allways.miner.optimizer.subscription_feed import FeedSession, Subscription, SubscriptionFeed

SUB_A = Subscription('a', 'accountSubscribe', ['A', {'encoding': 'base64'}])
SUB_B = Subscription('b', 'programSubscribe', ['B', {'encoding': 'base64'}])


def _ack(request, sub_id):
    return {'jsonrpc': '2.0', 'result': sub_id, 'id': request['id']}


def _note(sub_id, value):
    return {'jsonrpc': '2.0', 'method': 'accountNotification', 'params': {'subscription': sub_id, 'result': value}}


def test_session_routes_acks_by_request_id_and_notifications_by_subscription_id():
    session = FeedSession()
    req_a, req_b = session.request(SUB_A), session.request(SUB_B)
    assert session.handle_frame(_ack(req_b, 900)) is None  # acks may arrive out of order
    assert session.handle_frame(_ack(req_a, 800)) is None
    assert session.handle_frame(_note(900, {'value': 'b'})) == ('b', {'value': 'b'})
    assert session.handle_frame(_note(800, {'value': 'a'})) == ('a', {'value': 'a'})
    assert session.handle_frame(_note(7, {'value': '?'})) is None


def test_a_rejected_subscription_raises_so_the_session_is_replaced():
    session = FeedSession()
    request = session.request(SUB_A)
    with pytest.raises(ConnectionError):
        session.handle_frame({'jsonrpc': '2.0', 'error': {'code': -32602}, 'id': request['id']})


def test_live_only_while_every_subscription_is_acknowledged():
    feed = SubscriptionFeed('ws://x', lambda key, result: None)
    assert feed.add(SUB_A) and feed.add(SUB_B) and not feed.add(SUB_A)
    session = feed._current = FeedSession()
    req_a, req_b = session.request(SUB_A), session.request(SUB_B)
    session.handle_frame(_ack(req_a, 1))
    assert not feed.live
    session.handle_frame(_ack(req_b, 2))
    assert feed.live
    feed.add(Subscription('c', 'accountSubscribe', ['C']))
    assert not feed.live  # until the new subscription is acknowledged too
