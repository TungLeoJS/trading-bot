# vnstock_data live pytest design

## Goal

Create pytest coverage for functions exposed by `vnstock_data.show_api()`, organized by top-level module, using examples and constraints from repository docs.

## Scope

Cover live API calls for visible Unified UI modules:

- `Reference`
- `Market`
- `Insights`
- `Fundamental` when documented/testable methods are discoverable
- `Macro` and `Analytics` only if `show_doc()` or docs expose callable methods with safe sample parameters

Tests use the installed sponsored environment at `$HOME/.venv`. Current auth tier is `silver`, so `vnstock_data`, `vnstock_ta`, and `vnstock_news` are allowed.

## Test layout

Create `tests/vnstock_data/` with:

- `conftest.py`: shared fixtures, marker registration guidance, helper assertions
- `test_reference.py`: reference endpoints from `Reference`
- `test_market.py`: market endpoints from `Market`
- `test_insights.py`: insights endpoints from `Insights`
- `test_fundamental.py`: fundamental endpoints if method docs expose safe calls

## Test behavior

All network/API tests use `@pytest.mark.live`. They should run with:

```bash
PYTHONIOENCODING=utf-8 "$HOME/.venv/Scripts/python.exe" -m pytest -m live tests/vnstock_data
```

Assertions stay light because provider data changes over time:

- result is not `None`
- DataFrame/list/dict shape is valid
- DataFrame is non-empty when docs imply data must exist
- expected columns are asserted only when docs clearly state them

## Sample parameters

Use small, stable inputs:

- symbol: `VCB`
- group: `VN30`
- exchange: `HSX`
- fund symbol: picked from `Reference().fund.list()` if fund endpoint needs a valid fund code
- dates/limits: shortest documented range or low limit when supported

## Error handling

Tests should fail on unexpected exceptions. For endpoints marked experimental in `show_api()`, tests may use weaker assertions but should still fail if call contract breaks.

## Out of scope

- Snapshot schema testing
- Mocking provider responses
- Performance benchmarking
- Full historical data completeness checks
- Testing libraries outside `vnstock_data` unless needed as inputs
