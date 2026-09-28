import os
import tempfile
import unittest

from src.mobility_aggregate.store import CorruptLogError, EventStore


class EventStoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.store = EventStore(self.dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_append_and_replay(self):
        self.store.append_block([("e", {"v": 1})])
        self.store.append_block([("e", {"v": 2}), ("e", {"v": 3})])
        events = list(EventStore(self.dir).iter_events())
        self.assertEqual([e.payload["v"] for e in events], [1, 2, 3])
        self.assertEqual([e.seq for e in events], [1, 2, 3])
        # 两个事务块
        self.assertEqual(len([n for n in os.listdir(self.dir) if n.endswith(".jsonl")]), 2)

    def test_empty_block_rejected(self):
        with self.assertRaises(ValueError):
            self.store.append_block([])

    def test_reload_continues_chain(self):
        self.store.append_block([("e", {"v": 1})])
        head_after_first = self.store.head()
        reloaded = EventStore(self.dir)
        self.assertEqual(reloaded.head(), head_after_first)
        reloaded.append_block([("e", {"v": 2})])
        self.assertEqual(reloaded.seq(), 2)

    def test_tampered_event_hash_detected(self):
        self.store.append_block([("e", {"v": 1})])
        path = os.path.join(self.dir, "00000001.jsonl")
        with open(path, encoding="utf-8") as f:
            content = f.read().replace('"v":1', '"v":9')
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        with self.assertRaises(CorruptLogError):
            list(EventStore(self.dir).iter_events())

    def test_broken_chain_detected(self):
        self.store.append_block([("e", {"v": 1})])
        self.store.append_block([("e", {"v": 2})])
        path = os.path.join(self.dir, "00000002.jsonl")
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        lines[0] = lines[0].replace('"prev":"', '"prev":"00')  # 破坏块头链头
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        with self.assertRaises(CorruptLogError):
            list(EventStore(self.dir).iter_events())

    def test_missing_block_gap_detected(self):
        self.store.append_block([("e", {"v": 1})])
        os.rename(os.path.join(self.dir, "00000001.jsonl"),
                  os.path.join(self.dir, "00000002.jsonl"))
        with self.assertRaises(CorruptLogError):
            EventStore(self.dir)

    def test_leftover_tmp_ignored(self):
        self.store.append_block([("e", {"v": 1})])
        with open(os.path.join(self.dir, ".00000002.tmp"), "w", encoding="utf-8") as f:
            f.write('{"block": 2, "prev": "garbage"}\n{"half":')
        reloaded = EventStore(self.dir)  # 崩溃残块不影响打开
        self.assertEqual(reloaded.seq(), 1)
        # 新事务安全覆盖残块
        reloaded.append_block([("e", {"v": 2})])
        self.assertEqual([e.payload["v"] for e in EventStore(self.dir).iter_events()], [1, 2])


if __name__ == "__main__":
    unittest.main()
