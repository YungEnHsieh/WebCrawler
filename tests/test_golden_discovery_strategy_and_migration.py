from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from containers.scheduler_control.offerer.selection.golden_discovery_ranker_v1_strategy import (
    GoldenDiscoveryRankerV1Strategy,
)
from containers.scheduler_control.scorer import service as scorer_service
from containers.scheduler_control.scorer.service import (
    GoldenDiscoveryRankerConfig,
    GoldenDiscoveryRankerService,
)
from scripts import migrate_add_url_score_updated_at as migration


class GoldenDiscoveryMigrationSqlTest(unittest.TestCase):
    def test_unscored_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_unscored_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_unscored",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (first_seen ASC NULLS LAST)",
            sql,
        )
        self.assertIn(
            "WHERE should_crawl = TRUE AND url_score_updated_at IS NULL",
            sql,
        )
        self.assertNotIn("url_state_history", sql)

    def test_selection_index_is_partial_current_shard_index(self):
        sql = migration.create_golden_discovery_selection_index_sql(7)

        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "idx_url_state_current_007_golden_discovery_v1_selection",
            sql,
        )
        self.assertIn(
            "ON url_state_current_007 (domain_id, "
            "((CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END)), "
            "url_score DESC NULLS LAST, "
            "domain_score DESC NULLS LAST, "
            "last_scheduled ASC NULLS FIRST, "
            "first_seen ASC)",
            sql,
        )
        self.assertIn("WHERE should_crawl = TRUE", sql)
        self.assertNotIn("url_state_history", sql)

    def test_column_migration_targets_current_and_history_shards(self):
        tables = list(migration.iter_state_tables(num_shards=2))

        self.assertEqual(
            tables,
            [
                "url_state_current_000",
                "url_state_current_001",
                "url_state_history_000",
                "url_state_history_001",
            ],
        )
        self.assertEqual(
            migration.add_url_score_updated_at_sql("url_state_current_000"),
            "ALTER TABLE url_state_current_000 "
            "ADD COLUMN IF NOT EXISTS url_score_updated_at TIMESTAMPTZ",
        )


class _FakeResult:
    def fetchall(self):
        return [SimpleNamespace(domain_id=7, url="https://example.com/a")]


class _FakeOffererSession:
    def __init__(self):
        self.calls = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.calls.append((str(sql), params))
        return _FakeResult()

    def commit(self):
        self.committed = True


class _FakeOffererSessionFactory:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self.session


class GoldenDiscoveryOffererStrategyTest(unittest.TestCase):
    def test_strategy_prefers_refreshed_ranker_scores_in_sql(self):
        session = _FakeOffererSession()
        strategy = GoldenDiscoveryRankerV1Strategy(Session=_FakeOffererSessionFactory(session))

        selected = strategy.select_by_domain(
            shard_id=3,
            exclude_domain_ids={11, 22},
            per_domain_cap=2,
            max_domains=5,
        )

        self.assertEqual(selected, {7: ["https://example.com/a"]})
        self.assertTrue(session.committed)

        sql, params = session.calls[0]
        self.assertIn("FROM url_state_current_003", sql)
        self.assertIn(
            "MAX(CASE WHEN url_score_updated_at IS NOT NULL THEN url_score END)",
            sql,
        )
        self.assertNotIn("WHEN MAX(CASE", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("FROM domain_state d", sql)
        self.assertIn("d.crawl_paused_until > NOW()", sql)
        self.assertIn(
            "CASE WHEN url_score_updated_at IS NULL THEN 1 ELSE 0 END",
            sql,
        )
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params["exclude"], (11, 22))
        self.assertEqual(params["per_domain_cap"], 2)
        self.assertEqual(params["max_domains"], 5)


class _FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return [
            ("https://example.com/a",),
            ("https://example.com/b",),
        ]


class _FakeRawConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class _FakeSqlAlchemyConnection:
    def __init__(self, cursor):
        self.connection = _FakeRawConnection(cursor)


class _FakeScorerSession:
    def __init__(self, cursor):
        self._cursor = cursor

    def connection(self):
        return _FakeSqlAlchemyConnection(self._cursor)


class _FakeBeginContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeScorerSessionFactory:
    def __init__(self, cursor):
        self.session = _FakeScorerSession(cursor)

    def begin(self):
        return _FakeBeginContext(self.session)


class _FakeScorer:
    def score_many(self, urls):
        self.urls = urls
        return [0.25, 0.75]


class GoldenDiscoveryRankerServiceTest(unittest.TestCase):
    def test_scorer_claims_unscored_urls_and_updates_timestamp(self):
        cursor = _FakeCursor()
        fake_scorer = _FakeScorer()
        cfg = GoldenDiscoveryRankerConfig(
            total_shards=256,
            num_workers=4,
            worker_id=0,
            batch_size=1000,
            scan_interval_sec=60,
            max_batches_per_shard=1,
        )
        service = GoldenDiscoveryRankerService(
            cfg=cfg,
            Session=_FakeScorerSessionFactory(cursor),
            scorer=fake_scorer,
        )
        execute_values_calls = []

        def fake_execute_values(cur, sql, rows, page_size):
            execute_values_calls.append((cur, sql, rows, page_size))

        with patch.object(scorer_service, "execute_values", fake_execute_values):
            count = service._score_batch(5)

        self.assertEqual(count, 2)
        self.assertEqual(
            fake_scorer.urls,
            ["https://example.com/a", "https://example.com/b"],
        )

        select_sql, select_params = cursor.executed[0]
        self.assertIn("FROM url_state_current_005", select_sql)
        self.assertIn("url_score_updated_at IS NULL", select_sql)
        self.assertIn("ORDER BY first_seen ASC NULLS LAST", select_sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", select_sql)
        self.assertEqual(select_params, (1000,))

        _, update_sql, rows, page_size = execute_values_calls[0]
        self.assertIn("url_score_updated_at = CURRENT_TIMESTAMP", update_sql)
        self.assertEqual(
            rows,
            [
                ("https://example.com/a", 0.25),
                ("https://example.com/b", 0.75),
            ],
        )
        self.assertEqual(page_size, 2)


if __name__ == "__main__":
    unittest.main()
