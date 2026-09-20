"""Граф — один связный компонент от raw/* до mart_order_features (ADR-05); dbt-тесты — checks."""

from __future__ import annotations

import dagster as dg
import pytest

pytestmark = pytest.mark.smoke


@pytest.fixture(scope="module")
def graph():
    from olist_ml.definitions import defs

    return defs().resolve_asset_graph()


def test_mart_reachable_from_raw_orders(graph) -> None:
    start, target = dg.AssetKey(["raw", "orders"]), dg.AssetKey(["mart_order_features"])
    seen, frontier = {start}, [start]
    while frontier:
        key = frontier.pop()
        for nxt in graph.get(key).child_keys:
            if nxt not in seen:
                seen.add(nxt)
                frontier.append(nxt)
    assert target in seen


def test_layers_and_groups(graph) -> None:
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert {"stg_orders", "int_orders_enriched", "mart_order_features"} <= keys
    assert len([k for k in keys if k.startswith("raw/")]) == 8
    assert graph.get(dg.AssetKey(["stg_orders"])).group_name == "staging"
    assert graph.get(dg.AssetKey(["mart_order_features"])).group_name == "marts"
    assert not any(k.startswith("mart_") and k != "mart_order_features" for k in keys)


def test_mart_has_blocking_checks(graph) -> None:
    checks = [
        c for c in graph.asset_check_keys if c.asset_key == dg.AssetKey(["mart_order_features"])
    ]
    names = {c.name for c in checks}
    assert any("unique" in n and "order_id" in n for n in names), names
    assert any("accepted_range" in n and "freight_share" in n for n in names), names


def test_mart_has_freshness_policy_only(graph) -> None:
    import datetime as dt

    from olist_ml.settings import settings

    mart = graph.get(dg.AssetKey(["mart_order_features"]))
    policy = mart.freshness_policy
    assert policy is not None, "FreshnessPolicy.time_window на витрине (ADR-17)"
    fail = policy.fail_window
    assert (
        fail.days * 86400 + fail.seconds
        == dt.timedelta(minutes=settings.FRESHNESS_FAIL_MIN).total_seconds()
    )
    assert graph.get(dg.AssetKey(["stg_orders"])).freshness_policy is None
