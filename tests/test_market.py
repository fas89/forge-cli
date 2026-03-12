"""Tests for fluid_build.cli.market — enums, dataclasses, AdvancedSearchEngine, CircuitBreaker."""
import logging
import time
from datetime import datetime, timezone, timedelta

from fluid_build.cli.market import (
    CatalogType,
    DataProductLayer,
    DataProductStatus,
    DataProductMetadata,
    SearchFilters,
    SearchResult,
    AdvancedSearchEngine,
    CircuitBreaker,
)


def _make_product(**overrides):
    """Helper to create a DataProductMetadata with sensible defaults."""
    defaults = dict(
        id="p1", name="Sales Data", description="Daily sales", domain="finance",
        owner="alice", layer=DataProductLayer.GOLD, status=DataProductStatus.ACTIVE,
        version="1.0", created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc), tags=["sales", "gold"],
    )
    defaults.update(overrides)
    return DataProductMetadata(**defaults)


# ── Enums ──

class TestEnums:
    def test_catalog_type_values(self):
        assert CatalogType.GOOGLE_CLOUD_DATA_CATALOG.value == "google_cloud_data_catalog"
        assert CatalogType.FLUID_COMMAND_CENTER.value == "fluid_command_center"

    def test_data_product_layer_values(self):
        assert DataProductLayer.RAW.value == "raw"
        assert DataProductLayer.GOLD.value == "gold"

    def test_data_product_status_values(self):
        assert DataProductStatus.ACTIVE.value == "active"
        assert DataProductStatus.DEPRECATED.value == "deprecated"


# ── DataProductMetadata ──

class TestDataProductMetadata:
    def test_defaults(self):
        p = _make_product()
        assert p.tags == ["sales", "gold"]
        assert p.schema_url is None
        assert p.quality_score is None

    def test_custom_fields(self):
        p = _make_product(quality_score=0.95, catalog_source="bigquery")
        assert p.quality_score == 0.95
        assert p.catalog_source == "bigquery"


# ── SearchFilters ──

class TestSearchFilters:
    def test_defaults(self):
        f = SearchFilters()
        assert f.limit == 50
        assert f.offset == 0
        assert f.exact_match is False
        assert f.sort_by == "relevance"
        assert f.sort_order == "desc"
        assert f.search_fields == ["name", "description", "tags"]

    def test_custom(self):
        f = SearchFilters(domain="finance", layer=DataProductLayer.GOLD, limit=10)
        assert f.domain == "finance"
        assert f.layer == DataProductLayer.GOLD
        assert f.limit == 10


# ── SearchResult ──

class TestSearchResult:
    def test_basic(self):
        products = [_make_product()]
        sr = SearchResult(products=products, total_count=1, facets={}, query_time=0.01)
        assert sr.total_count == 1
        assert sr.suggestions == []


# ── AdvancedSearchEngine ──

class TestAdvancedSearchEngine:
    def _engine(self):
        return AdvancedSearchEngine(logging.getLogger("test"))

    # -- relevance scoring --

    def test_relevance_no_query(self):
        engine = self._engine()
        f = SearchFilters()  # no text_query
        score = engine.calculate_relevance_score(_make_product(), f)
        assert score == 1.0

    def test_relevance_name_match(self):
        engine = self._engine()
        f = SearchFilters(text_query="Sales")
        score = engine.calculate_relevance_score(_make_product(name="Sales Data"), f)
        assert score > 0

    def test_relevance_quality_boost(self):
        engine = self._engine()
        f = SearchFilters(text_query="Sales")
        base = engine.calculate_relevance_score(_make_product(quality_score=None), f)
        boosted = engine.calculate_relevance_score(_make_product(quality_score=1.0), f)
        assert boosted > base

    def test_relevance_recency_boost(self):
        engine = self._engine()
        f = SearchFilters(text_query="Sales")
        old = _make_product(updated_at=datetime.now(timezone.utc) - timedelta(days=365))
        new = _make_product(updated_at=datetime.now(timezone.utc))
        score_old = engine.calculate_relevance_score(old, f)
        score_new = engine.calculate_relevance_score(new, f)
        assert score_new >= score_old

    # -- facet extraction --

    def test_extract_facets(self):
        engine = self._engine()
        products = [
            _make_product(domain="finance", tags=["a"]),
            _make_product(domain="finance", tags=["a", "b"]),
            _make_product(domain="marketing", tags=["b"]),
        ]
        facets = engine.extract_facets(products)
        assert facets["domain"]["finance"] == 2
        assert facets["domain"]["marketing"] == 1
        assert facets["tags"]["a"] == 2
        assert facets["tags"]["b"] == 2

    # -- advanced filters --

    def test_exclude_deprecated(self):
        engine = self._engine()
        products = [
            _make_product(status=DataProductStatus.ACTIVE),
            _make_product(status=DataProductStatus.DEPRECATED),
        ]
        f = SearchFilters(include_deprecated=False)
        result = engine.apply_advanced_filters(products, f)
        assert len(result) == 1

    def test_filter_has_documentation(self):
        engine = self._engine()
        products = [
            _make_product(documentation_url="https://docs.example.com"),
            _make_product(documentation_url=None),
        ]
        f = SearchFilters(has_documentation=True)
        result = engine.apply_advanced_filters(products, f)
        assert len(result) == 1

    def test_filter_facets_domain(self):
        engine = self._engine()
        products = [
            _make_product(domain="finance"),
            _make_product(domain="marketing"),
        ]
        f = SearchFilters(facets={"domain": ["finance"]})
        result = engine.apply_advanced_filters(products, f)
        assert len(result) == 1

    def test_filter_usage_count(self):
        engine = self._engine()
        products = [
            _make_product(usage_stats={"total_queries": 100}),
            _make_product(usage_stats={"total_queries": 5}),
        ]
        f = SearchFilters(min_usage_count=10)
        result = engine.apply_advanced_filters(products, f)
        assert len(result) == 1

    # -- ranking & sorting --

    def test_sort_by_name_asc(self):
        engine = self._engine()
        products = [_make_product(name="Zebra"), _make_product(name="Alpha")]
        f = SearchFilters(sort_by="name", sort_order="asc")
        result = engine.rank_and_sort_products(products, f)
        assert result[0].name == "Alpha"

    def test_sort_by_quality_desc(self):
        engine = self._engine()
        products = [
            _make_product(quality_score=0.5),
            _make_product(quality_score=0.9),
        ]
        f = SearchFilters(sort_by="quality_score", sort_order="desc")
        result = engine.rank_and_sort_products(products, f)
        assert result[0].quality_score == 0.9

    def test_sort_by_relevance_with_query(self):
        engine = self._engine()
        products = [
            _make_product(name="Unrelated"),
            _make_product(name="Sales Report"),
        ]
        f = SearchFilters(text_query="Sales", sort_by="relevance", sort_order="desc")
        result = engine.rank_and_sort_products(products, f)
        assert result[0].name == "Sales Report"

    # -- saved searches --

    def test_save_and_load_search(self):
        engine = self._engine()
        f = SearchFilters(search_name="my-search", domain="finance")
        assert engine.save_search(f) is True
        loaded = engine.load_saved_search("my-search")
        assert loaded is not None
        assert loaded.domain == "finance"

    def test_save_search_without_name(self):
        engine = self._engine()
        f = SearchFilters()
        assert engine.save_search(f) is False

    def test_list_saved_searches(self):
        engine = self._engine()
        engine.save_search(SearchFilters(search_name="s1"))
        engine.save_search(SearchFilters(search_name="s2"))
        assert sorted(engine.list_saved_searches()) == ["s1", "s2"]

    # -- suggestions --

    def test_generate_suggestions(self):
        engine = self._engine()
        products = [_make_product(name="Sales Report", tags=["revenue", "daily"])]
        suggestions = engine.generate_search_suggestions(products, "sal")
        assert any("sales" in s for s in suggestions)


# ── CircuitBreaker ──

class TestCircuitBreaker:
    def test_initial_state(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10)
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0

    def test_on_success_resets(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10)
        cb.failure_count = 2
        cb.state = "HALF_OPEN"
        cb._on_success()
        assert cb.failure_count == 0
        assert cb.state == "CLOSED"

    def test_on_failure_increments(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10)
        cb._on_failure()
        assert cb.failure_count == 1
        assert cb.state == "CLOSED"

    def test_on_failure_opens_at_threshold(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10)
        cb._on_failure()
        cb._on_failure()
        assert cb.state == "OPEN"

    def test_should_attempt_reset_false(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=60)
        cb.last_failure_time = time.time()
        assert cb._should_attempt_reset() is False

    def test_should_attempt_reset_true(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        cb.last_failure_time = time.time() - 2
        assert cb._should_attempt_reset() is True

    def test_should_attempt_reset_no_failure(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        assert not cb._should_attempt_reset()
