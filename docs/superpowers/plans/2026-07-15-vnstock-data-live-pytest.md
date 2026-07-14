# vnstock_data Live Pytest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build live pytest coverage for `vnstock_data.show_api()` endpoints, split by module file.

**Architecture:** Tests live in `tests/vnstock_data/` and use `@pytest.mark.live` so provider-backed calls are explicit. Shared fixtures create `vnstock_data` clients once per test session, and helper assertions validate changing provider data without brittle schema snapshots.

**Tech Stack:** Python, pytest, pandas, `vnstock_data` installed in `$HOME/.venv` on Windows.

## Global Constraints

- Use `$HOME/.venv/Scripts/python.exe` for test execution.
- Current license tier is `silver`; `vnstock_data`, `vnstock_ta`, and `vnstock_news` are allowed.
- Network/API tests must use `@pytest.mark.live`.
- Assertions must stay light: result is not `None`, table/list/dict shape is valid, non-empty only where docs imply data exists.
- Do not mock provider responses.
- Do not add snapshot schema tests.
- Do not test libraries outside `vnstock_data`.
- Before editing any existing function/class/method, run GitNexus `impact`; this plan creates new test files and one new pytest config only.
- Do not commit implementation changes unless user explicitly authorizes commits for implementation work.

---

## File Structure

Create these files:

- `pytest.ini`: registers `live` marker and test discovery.
- `tests/vnstock_data/conftest.py`: imports `vnstock_data`, client fixtures, `assert_table_like()`, `extract_first_symbol()`.
- `tests/vnstock_data/test_reference.py`: covers `Reference` functions visible in `show_api()`.
- `tests/vnstock_data/test_market.py`: covers `Market.odd_lot()` plus docs-visible `Market.quote()` and `Market.equity().ohlcv()` as safe market smoke tests.
- `tests/vnstock_data/test_fundamental.py`: covers `Fundamental().equity("VCB")` methods from `show_doc("Fundamental")`.
- `tests/vnstock_data/test_insights.py`: covers `Insights` functions visible in `show_api()`.
- `tests/vnstock_data/test_analytics.py`: covers docs-visible `Analytics().valuation("VNINDEX")` methods.
- `tests/vnstock_data/test_macro.py`: covers docs-visible `Macro` economy, commodity, and currency methods with weak live assertions.

---

### Task 1: Pytest Config And Shared Fixtures

**Files:**
- Create: `pytest.ini`
- Create: `tests/vnstock_data/conftest.py`

**Interfaces:**
- Consumes: `vnstock_data` package from `$HOME/.venv`.
- Produces:
  - `assert_table_like(value, allow_empty=False) -> object`
  - `extract_first_symbol(value, candidates) -> str`
  - pytest fixtures: `reference`, `market`, `fundamental`, `insights`, `analytics`, `macro`

- [ ] **Step 1: Write pytest config**

Create `pytest.ini`:

```ini
[pytest]
testpaths = tests
markers =
    live: tests that call live vnstock_data provider APIs
```

- [ ] **Step 2: Write shared fixtures and assertions**

Create `tests/vnstock_data/conftest.py`:

```python
import pytest


def assert_table_like(value, allow_empty=False):
    assert value is not None

    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None

    if pd is not None and isinstance(value, pd.DataFrame):
        assert value.shape[1] > 0
        if not allow_empty:
            assert not value.empty
        return value

    if isinstance(value, dict):
        if not allow_empty:
            assert len(value) > 0
        return value

    if isinstance(value, (list, tuple, set)):
        if not allow_empty:
            assert len(value) > 0
        return value

    if hasattr(value, "__len__") and not isinstance(value, (str, bytes)):
        if not allow_empty:
            assert len(value) > 0
        return value

    return value


def extract_first_symbol(value, candidates):
    table = assert_table_like(value)

    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None

    if pd is not None and isinstance(table, pd.DataFrame):
        for column in candidates:
            if column in table.columns and not table[column].dropna().empty:
                return str(table[column].dropna().iloc[0])
        first_row = table.dropna(how="all").iloc[0]
        for item in first_row:
            if item:
                return str(item)

    if isinstance(table, dict):
        for key in candidates:
            if key in table and table[key]:
                value = table[key]
                if isinstance(value, (list, tuple)):
                    return str(value[0])
                return str(value)
        first_value = next(iter(table.values()))
        if isinstance(first_value, (list, tuple)):
            return str(first_value[0])
        return str(first_value)

    if isinstance(table, (list, tuple, set)):
        first_value = next(iter(table))
        if isinstance(first_value, dict):
            for key in candidates:
                if first_value.get(key):
                    return str(first_value[key])
        return str(first_value)

    return str(table)


@pytest.fixture(scope="session")
def reference():
    from vnstock_data import Reference

    return Reference()


@pytest.fixture(scope="session")
def market():
    from vnstock_data import Market

    return Market()


@pytest.fixture(scope="session")
def fundamental():
    from vnstock_data import Fundamental

    return Fundamental(source="mas")


@pytest.fixture(scope="session")
def insights():
    from vnstock_data import Insights

    return Insights()


@pytest.fixture(scope="session")
def analytics():
    from vnstock_data import Analytics

    return Analytics()


@pytest.fixture(scope="session")
def macro():
    from vnstock_data import Macro

    return Macro()
```

- [ ] **Step 3: Run config-only collection**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest --collect-only tests/vnstock_data -q
```

Expected: collection succeeds, likely `no tests collected` until later tasks add test files.

---

### Task 2: Reference Module Live Tests

**Files:**
- Create: `tests/vnstock_data/test_reference.py`

**Interfaces:**
- Consumes: `reference` fixture, `assert_table_like()`, `extract_first_symbol()` from `conftest.py`.
- Produces: live pytest coverage for `Reference.equity`, `Reference.etf`, `Reference.events`, `Reference.fund`, `Reference.index`, `Reference.industry`, `Reference.market`, and `Reference.search`.

- [ ] **Step 1: Write Reference tests**

Create `tests/vnstock_data/test_reference.py`:

```python
import pytest

from .conftest import assert_table_like, extract_first_symbol

pytestmark = pytest.mark.live


def test_reference_equity_list(reference):
    assert_table_like(reference.equity.list())


def test_reference_equity_list_by_exchange(reference):
    assert_table_like(reference.equity.list_by_exchange())


def test_reference_equity_list_by_group(reference):
    assert_table_like(reference.equity.list_by_group("VN30"))


def test_reference_equity_list_by_industry(reference):
    assert_table_like(reference.equity.list_by_industry(lang="vi"))


def test_reference_etf_list(reference):
    assert_table_like(reference.etf.list(), allow_empty=True)


def test_reference_events_calendar(reference):
    result = reference.events.calendar(start="2026-01-01", end="2026-01-31", limit=5)
    assert_table_like(result, allow_empty=True)


def test_reference_fund_list(reference):
    assert_table_like(reference.fund.list())


def test_reference_fund_detail_endpoints(reference):
    fund_symbol = extract_first_symbol(
        reference.fund.list(),
        candidates=("symbol", "code", "short_name", "shortName", "fund_code", "fundCode"),
    )

    assert_table_like(reference.fund.asset_holding(fund_symbol), allow_empty=True)
    assert_table_like(reference.fund.industry_holding(fund_symbol), allow_empty=True)
    assert_table_like(reference.fund.nav_report(fund_symbol), allow_empty=True)
    assert_table_like(reference.fund.top_holding(fund_symbol), allow_empty=True)


def test_reference_index_groups(reference):
    assert_table_like(reference.index.groups())


def test_reference_industry_list(reference):
    assert_table_like(reference.industry.list(lang="vi"))


def test_reference_industry_sectors(reference):
    assert_table_like(reference.industry.sectors(lang="vi"))


def test_reference_market_status(reference):
    assert_table_like(reference.market.status(), allow_empty=True)


def test_reference_search_symbol(reference):
    assert_table_like(reference.search.symbol("VCB", limit=5), allow_empty=True)


def test_reference_search_info(reference):
    assert_table_like(reference.search.info("VCB", limit=5), allow_empty=True)
```

- [ ] **Step 2: Run Reference tests**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data/test_reference.py -q
```

Expected: all Reference tests pass. If one provider returns empty data for date-sensitive endpoints, keep `allow_empty=True` only for that endpoint.

---

### Task 3: Market And Fundamental Live Tests

**Files:**
- Create: `tests/vnstock_data/test_market.py`
- Create: `tests/vnstock_data/test_fundamental.py`

**Interfaces:**
- Consumes: `market`, `fundamental`, `assert_table_like()`.
- Produces: live pytest coverage for `Market.odd_lot()` and `Fundamental().equity("VCB")` methods.

- [ ] **Step 1: Write Market tests**

Create `tests/vnstock_data/test_market.py`:

```python
import pytest

from .conftest import assert_table_like

pytestmark = pytest.mark.live


def test_market_odd_lot(market):
    assert_table_like(market.odd_lot(["VCB"]), allow_empty=True)


def test_market_quote(market):
    assert_table_like(market.quote(["VCB"]), allow_empty=True)


def test_market_equity_ohlcv(market):
    stock = market.equity("VCB")
    assert_table_like(stock.ohlcv(), allow_empty=True)
```

- [ ] **Step 2: Write Fundamental tests**

Create `tests/vnstock_data/test_fundamental.py`:

```python
import pytest

from .conftest import assert_table_like

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def vcb_fundamental(fundamental):
    return fundamental.equity("VCB")


@pytest.mark.parametrize(
    ("method_name", "kwargs", "allow_empty"),
    [
        ("ratio", {"period": "year", "limit": 4}, False),
        ("income_statement", {"period": "year", "limit": 4}, False),
        ("balance_sheet", {"period": "year", "limit": 4}, False),
        ("cash_flow", {"period": "year", "limit": 4}, False),
        ("note", {}, True),
        ("filing", {"doc_type": None}, True),
        ("financial_health", {"scorecard": "auto", "lang": "vi", "limit": 4}, False),
    ],
)
def test_fundamental_equity_methods(vcb_fundamental, method_name, kwargs, allow_empty):
    result = getattr(vcb_fundamental, method_name)(**kwargs)
    assert_table_like(result, allow_empty=allow_empty)
```

- [ ] **Step 3: Run Market and Fundamental tests**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data/test_market.py tests/vnstock_data/test_fundamental.py -q
```

Expected: all Market and Fundamental tests pass. If `Market().equity("VCB").ohlcv()` needs explicit date kwargs in this installed version, change that test call to `stock.ohlcv(start="2026-01-01", end="2026-01-31")` and rerun.

---

### Task 4: Insights Module Live Tests

**Files:**
- Create: `tests/vnstock_data/test_insights.py`

**Interfaces:**
- Consumes: `insights`, `assert_table_like()`.
- Produces: live pytest coverage for all `Insights` functions visible in `show_api()`.

- [ ] **Step 1: Write Insights tests**

Create `tests/vnstock_data/test_insights.py`:

```python
import pytest

from .conftest import assert_table_like

pytestmark = pytest.mark.live


@pytest.mark.parametrize(
    "method_name",
    ["active", "foreign", "proprietary"],
)
def test_insights_flow_methods(insights, method_name):
    result = getattr(insights.flow, method_name)(exchange="HOSE", group_by="stock")
    assert_table_like(result, allow_empty=True)


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        ("deal", {"index": "VNINDEX", "limit": 10}),
        ("foreign_buy", {"limit": 10}),
        ("foreign_sell", {"limit": 10}),
        ("gainer", {"index": "VNINDEX", "limit": 10}),
        ("loser", {"index": "VNINDEX", "limit": 10}),
        ("value", {"index": "VNINDEX", "limit": 10}),
        ("volume", {"index": "VNINDEX", "limit": 10}),
    ],
)
def test_insights_ranking_methods(insights, method_name, kwargs):
    result = getattr(insights.ranking, method_name)(**kwargs)
    assert_table_like(result, allow_empty=True)


def test_insights_screener_criteria(insights):
    assert_table_like(insights.screener.criteria(lang="vi"), allow_empty=True)


def test_insights_screener_filter(insights):
    assert_table_like(insights.screener.filter(limit=10), allow_empty=True)


@pytest.mark.parametrize(
    "method_name",
    ["breadth", "contribution", "heatmap"],
)
def test_insights_sentiment_methods(insights, method_name):
    result = getattr(insights.sentiment, method_name)(exchange="HOSE")
    assert_table_like(result, allow_empty=True)
```

- [ ] **Step 2: Run Insights tests**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data/test_insights.py -q
```

Expected: all Insights tests pass. Experimental endpoints may return empty data, but must not break call contract.

---

### Task 5: Analytics And Macro Docs-Visible Live Tests

**Files:**
- Create: `tests/vnstock_data/test_analytics.py`
- Create: `tests/vnstock_data/test_macro.py`

**Interfaces:**
- Consumes: `analytics`, `macro`, `assert_table_like()`.
- Produces: live pytest coverage for `Analytics` and `Macro` methods exposed by `show_doc()` and documented in `docs/vnstock-data`.

- [ ] **Step 1: Write Analytics tests**

Create `tests/vnstock_data/test_analytics.py`:

```python
import pytest

from .conftest import assert_table_like

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def vnindex_valuation(analytics):
    return analytics.valuation("VNINDEX")


@pytest.mark.parametrize("method_name", ["pe", "pb", "evaluation"])
def test_analytics_valuation_methods(vnindex_valuation, method_name):
    result = getattr(vnindex_valuation, method_name)(duration="1Y")
    assert_table_like(result, allow_empty=True)
```

- [ ] **Step 2: Write Macro tests**

Create `tests/vnstock_data/test_macro.py`:

```python
import pytest

from .conftest import assert_table_like

pytestmark = pytest.mark.live


@pytest.mark.parametrize(
    ("domain_name", "method_name", "kwargs"),
    [
        ("economy", "gdp", {}),
        ("economy", "cpi", {}),
        ("economy", "industry_prod", {}),
        ("economy", "import_export", {}),
        ("economy", "retail", {}),
        ("economy", "fdi", {}),
        ("economy", "money_supply", {}),
        ("economy", "population_labor", {}),
        ("economy", "credit", {}),
        ("economy", "total_investment", {}),
        ("economy", "state_budget", {}),
        ("commodity", "gold", {"market": "VN"}),
        ("commodity", "gas", {"market": "VN"}),
        ("commodity", "oil_crude", {}),
        ("commodity", "coke", {}),
        ("commodity", "steel", {"market": "GLOBAL"}),
        ("commodity", "iron_ore", {}),
        ("commodity", "fertilizer_ure", {}),
        ("commodity", "soybean", {}),
        ("commodity", "corn", {}),
        ("commodity", "sugar", {}),
        ("commodity", "pork", {"market": "VN"}),
        ("currency", "exchange_rate", {}),
        ("currency", "interest_rate", {}),
        ("currency", "interbank_rate", {}),
        ("currency", "policy_rate", {}),
        ("currency", "omo", {}),
        ("currency", "deposit_rate", {}),
    ],
)
def test_macro_methods(macro, domain_name, method_name, kwargs):
    domain = getattr(macro, domain_name)()
    result = getattr(domain, method_name)(**kwargs)
    assert_table_like(result, allow_empty=True)
```

- [ ] **Step 3: Run Analytics and Macro tests**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data/test_analytics.py tests/vnstock_data/test_macro.py -q
```

Expected: all Analytics and Macro tests pass. If a macro endpoint has provider outage but other endpoints work, mark only that parameter case with `pytest.param(..., marks=pytest.mark.xfail(reason="provider endpoint unavailable"))` after verifying failure is provider-side, not test code.

---

### Task 6: Full Live Suite Verification

**Files:**
- Modify only files created in Tasks 1-5 if verification reveals signature mismatches.

**Interfaces:**
- Consumes: all test files.
- Produces: verified live pytest suite.

- [ ] **Step 1: Run full live suite**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data -q
```

Expected: all tests pass.

- [ ] **Step 2: Run collection without live execution**

Run:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest --collect-only tests/vnstock_data -q
```

Expected: pytest lists tests from all module files with no import errors.

- [ ] **Step 3: Run GitNexus change detection**

Run GitNexus MCP:

```text
detect_changes({"scope": "all", "repo": "trading-bot"})
```

Expected: no production symbols changed; affected risk should be none/low because only tests and pytest config were added.

- [ ] **Step 4: Review git status**

Run:

```bash
git status --short
```

Expected: new files under `tests/vnstock_data/` and `pytest.ini`. Existing unrelated files such as `.idea/`, `vnstock-cli-installer.run`, or `.claude/settings.local.json` must not be staged.

- [ ] **Step 5: Commit only if user authorized implementation commits**

If the user explicitly authorized implementation commits, run:

```bash
git add pytest.ini tests/vnstock_data/conftest.py tests/vnstock_data/test_reference.py tests/vnstock_data/test_market.py tests/vnstock_data/test_fundamental.py tests/vnstock_data/test_insights.py tests/vnstock_data/test_analytics.py tests/vnstock_data/test_macro.py
git commit -m "$(cat <<'EOF'
Add live vnstock_data pytest coverage

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

Expected: commit succeeds without staging unrelated local files.

---

## Self-Review

- Spec coverage: plan covers module-split pytest files, live marker, `$HOME/.venv`, light assertions, Reference/Market/Insights/Fundamental, and docs-visible Analytics/Macro.
- Placeholder scan: no TBD/TODO/later placeholders remain.
- Type consistency: helpers and fixtures named in later tasks are defined in Task 1 with matching names.
- Scope check: one focused implementation plan; no independent subsystem split needed.
