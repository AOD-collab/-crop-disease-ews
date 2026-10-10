"""kafka.producer.ResilientBackend: an upload published while Kafka is down
must reach Kafka once the broker is back — not sit forever in the local file
queue (which Spark never reads) while the image shows INGESTED."""

import kafka.producer as producer


class FakeKafka:
    def __init__(self, topic):
        self.sent = []
        self.fail = False

    def send(self, message):
        if self.fail:
            raise RuntimeError("broker gone")
        self.sent.append(message)


def _backend(monkeypatch, tmp_path, broker_up):
    monkeypatch.setattr(producer, "LOCAL_QUEUE_DIR", tmp_path)
    state = {"up": broker_up, "instance": None}

    def fake_kafka(topic):
        if not state["up"]:
            raise RuntimeError("unreachable")
        state["instance"] = FakeKafka(topic)
        return state["instance"]

    monkeypatch.setattr(producer, "KafkaBackend", fake_kafka)
    b = producer.ResilientBackend("t")
    return b, state


def test_down_then_up_forwards_backlog_in_order(monkeypatch, tmp_path):
    b, state = _backend(monkeypatch, tmp_path, broker_up=False)
    assert b.active_name == "LocalFileBackend"
    b.send({"image_id": "a"})
    b.send({"image_id": "b"})
    assert [m["image_id"] for m in b._local.read_all()] == ["a", "b"]

    state["up"] = True
    b._next_attempt = 0  # skip the retry cooldown
    b.send({"image_id": "c"})

    assert b.active_name == "KafkaBackend"
    assert [m["image_id"] for m in state["instance"].sent] == ["a", "b", "c"]
    assert not b._local.path.exists()
    assert list(tmp_path.iterdir()) == []  # no leftover .draining files


def test_drain_without_new_send(monkeypatch, tmp_path):
    b, state = _backend(monkeypatch, tmp_path, broker_up=False)
    b.send({"image_id": "a"})
    assert b.drain() == 0  # still down (cooldown)

    state["up"] = True
    b._next_attempt = 0
    assert b.drain() == 1
    assert state["instance"].sent == [{"image_id": "a"}]


def test_broker_dies_after_connect_falls_back_without_losing_events(monkeypatch, tmp_path):
    b, state = _backend(monkeypatch, tmp_path, broker_up=True)
    state["instance"].fail = True
    b.send({"image_id": "x"})  # must not raise
    assert b.active_name == "LocalFileBackend"
    assert b._local.read_all() == [{"image_id": "x"}]


def test_partial_drain_failure_requeues_remainder(monkeypatch, tmp_path):
    b, state = _backend(monkeypatch, tmp_path, broker_up=True)
    for i in "abc":
        b._local.send({"image_id": i})
    kafka = state["instance"]
    real_send = kafka.send

    def flaky(m):
        if m["image_id"] == "b":
            raise RuntimeError("drop")
        real_send(m)

    kafka.send = flaky
    assert b.drain() == 1
    assert kafka.sent == [{"image_id": "a"}]
    assert [m["image_id"] for m in b._local.read_all()] == ["b", "c"]


def test_half_written_line_does_not_lose_the_backlog(monkeypatch, tmp_path):
    """A process killed mid-append leaves a broken last line. Before, drain()
    crashed on it, the claimed file was orphaned (its good events never sent)
    and the healthy broker was treated as down."""
    b, state = _backend(monkeypatch, tmp_path, broker_up=True)
    b._local.path.write_text('{"image_id": "a"}\n{"image_id": "b"}\n{"image_id": "c", "use')
    b.send({"image_id": "d"})
    assert [m["image_id"] for m in state["instance"].sent] == ["a", "b", "d"]
    assert b.active_name == "KafkaBackend"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["t.corrupt"]
    assert '"image_id": "c"' in (tmp_path / "t.corrupt").read_text()


def test_dead_broker_costs_one_timeout_not_two(monkeypatch, tmp_path):
    """With a backlog queued and the broker gone, drain() already waits out
    one delivery timeout; the new event must not wait out a second one."""
    b, state = _backend(monkeypatch, tmp_path, broker_up=True)
    b._local.send({"image_id": "old"})
    kafka = state["instance"]
    kafka.fail = True
    attempts = []
    real_send = kafka.send
    kafka.send = lambda m: (attempts.append(m["image_id"]), real_send(m))
    b.send({"image_id": "new"})
    assert attempts == ["old"]
    assert [m["image_id"] for m in b._local.read_all()] == ["old", "new"]


def test_orphaned_claim_from_a_crashed_drain_is_forwarded(monkeypatch, tmp_path):
    import os
    import time
    b, state = _backend(monkeypatch, tmp_path, broker_up=True)
    orphan = tmp_path / "t.jsonl.draining-deadbeef"
    orphan.write_text('{"image_id": "lost"}\n')
    fresh = tmp_path / "t.jsonl.draining-cafe"          # another process draining right now
    fresh.write_text('{"image_id": "busy"}\n')
    old = time.time() - b.ORPHAN_CLAIM_SECONDS - 5
    os.utime(orphan, (old, old))
    assert b.drain() == 1
    assert state["instance"].sent == [{"image_id": "lost"}]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["t.jsonl.draining-cafe"]
