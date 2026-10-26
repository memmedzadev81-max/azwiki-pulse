"""Tests. Run with:  python -m unittest -v"""
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from pulse import config
from pulse.bronze import BronzeWriter, Checkpoint
from pulse.contract import ContractError, to_silver_row
from pulse.ingest import consume
from pulse.process import process
from pulse.quality import DataQualityError
from pulse.sample import write_sample_bronze
from pulse.sse import parse_sse

T0 = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)


def make_event(eid, title="Bakı", user="Aysel", ts=T0, etype="edit", **extra):
    evt = {"meta": {"id": eid}, "type": etype, "namespace": 0, "title": title, "user": user,
           "timestamp": int(ts.timestamp()), "bot": False, "minor": False, "wiki": "azwiki",
           "comment": "", "length": {"old": 100, "new": 150}}
    evt.update(extra)
    return evt


def envelope(evt, ingested=T0):
    return {"ingested_at": ingested.isoformat(), "sse_id": "x", "event": evt}


class TmpCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.bronze, self.db = self.root / "bronze", self.root / "w.db"

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, envelopes, minute=0):
        BronzeWriter(self.bronze).write_batch(envelopes, now=T0.replace(minute=minute))

    def run_process(self):
        return process(self.db, self.bronze)

    def q(self, sql, params=()):
        con = sqlite3.connect(self.db)
        try:
            return con.execute(sql, params).fetchall()
        finally:
            con.close()


class TestSSE(unittest.TestCase):
    def test_parses_events_comments_and_multiline_data(self):
        lines = [":heartbeat", "event: message", "id: [1]", 'data: {"a":', 'data: 1}', "",
                 "data: second", ""]
        events = list(parse_sse(lines))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].id, "[1]")
        self.assertEqual(json.loads(events[0].data), {"a": 1})
        self.assertEqual(events[1].data, "second")


class TestContract(unittest.TestCase):
    def test_valid_event(self):
        row = to_silver_row(envelope(make_event("e1", comment="Redaktə geri qaytarıldı")), "f")
        self.assertEqual(row["bytes_delta"], 50)
        self.assertEqual(row["is_revert"], 1)

    def test_rejections(self):
        cases = {
            "missing_title": make_event("e", title=""),
            "wrong_wiki": make_event("e", wiki="enwiki"),
            "bad_timestamp": make_event("e", timestamp="abc"),
            "unknown_type": make_event("e", etype="weird"),
            "move_without_target": make_event("e", etype="log", log_type="move", log_params={}),
        }
        for expected, evt in cases.items():
            with self.subTest(expected):
                with self.assertRaises(ContractError) as ctx:
                    to_silver_row(envelope(evt), "f")
                self.assertEqual(str(ctx.exception), expected)


class FakeResponse:
    def __init__(self, lines):
        self.lines = lines

    def iter_lines(self, decode_unicode=True):
        return iter(self.lines)


class TestIngest(TmpCase):
    def sse_lines(self, events):
        out = []
        for i, evt in enumerate(events):
            out += [f"id: [{i}]", "data: " + json.dumps(evt), ""]
        return out

    def test_filters_wiki_and_canary_and_checkpoints(self):
        events = [make_event("a"), make_event("b", wiki="enwiki"),
                  dict(make_event("c"), meta={"id": "c", "domain": "canary"}), make_event("d")]
        cp = Checkpoint(self.root / "cp.json")
        stats = consume(minutes=1, max_events=2, writer=BronzeWriter(self.bronze), checkpoint=cp,
                        stream_opener=lambda last_id: FakeResponse(self.sse_lines(events)))
        self.assertEqual(stats["kept"], 2)
        lines = next(self.bronze.rglob("*.jsonl")).read_text(encoding="utf-8").splitlines()
        self.assertEqual([json.loads(l)["event"]["meta"]["id"] for l in lines], ["a", "d"])
        self.assertEqual(cp.load(), "[3]")


class TestPipeline(TmpCase):
    def test_duplicates_and_dead_letters(self):
        self.write([envelope(make_event("a")), envelope(make_event("a")),
                    envelope(make_event("b", title=""))])
        self.write([envelope(make_event("a"))], minute=5)
        summary = self.run_process()
        self.assertEqual(summary["inserted"], 1)
        self.assertEqual(summary["duplicates"], 2)
        self.assertEqual(summary["dead"], 1)

    def test_rerun_processes_nothing_new(self):
        self.write([envelope(make_event("a"))])
        self.run_process()
        summary = self.run_process()
        self.assertEqual((summary["files"], summary["fact_rows"]), (0, 0))

    def test_rename_creates_scd2_versions_with_same_page_nk(self):
        t1, t2, t3 = T0, T0.replace(minute=10), T0.replace(minute=20)
        self.write([envelope(make_event("a", title="Gəncə şəhəri", ts=t1)),
                    envelope(make_event("m", title="Gəncə şəhəri", ts=t2, etype="log",
                                        log_type="move", log_params={"target": "Gəncə"})),
                    envelope(make_event("b", title="Gəncə", ts=t3))])
        self.run_process()
        rows = self.q("SELECT page_nk, title, is_current FROM dim_page ORDER BY page_sk")
        self.assertEqual(rows, [(1, "Gəncə şəhəri", 0), (1, "Gəncə", 1)])

    def test_quality_failure_rolls_back(self):
        self.write([envelope(make_event("a"))])
        with mock.patch("pulse.process.run_checks", side_effect=DataQualityError("boom")):
            with self.assertRaises(DataQualityError):
                self.run_process()
        self.assertEqual(self.q("SELECT COUNT(*) FROM silver_events"), [(0,)])
        self.assertEqual(self.q("SELECT status FROM etl_runs"), [("failed",)])


class TestSampleScenario(TmpCase):
    def test_demo_finds_planted_spike_and_edit_war(self):
        write_sample_bronze(self.bronze, hours=36, seed=7)
        self.run_process()
        alerts = {(a, t) for a, t in self.q("SELECT alert_type, title FROM alerts")}
        self.assertIn(("spike", "Neftçi (futbol klubu)"), alerts)
        self.assertIn(("edit_war", "Xəzər dənizi"), alerts)


if __name__ == "__main__":
    unittest.main()
