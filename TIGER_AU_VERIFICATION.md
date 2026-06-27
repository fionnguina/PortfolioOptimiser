# Tiger Brokers AU — Pre-Commitment Verification Checklist

**Status**: PROVISIONAL — `tiger_au` profile added to `BROKER_PROFILES`
with best-guess fees from training-data recollection. Do not rely on
the engine's Tiger backtests for live decisions until everything below
is verified directly with Tiger Brokers Australia.

**Why this matters**: Saxo Bank Simulation was eliminated as a real
alternative because it caps at 20 days / USD 100k. We need to confirm
Tiger AU doesn't have analogous restrictions before pursuing the
OpenAPI port (~1-2 days of engineering).

## Critical (deal-breakers)

These determine whether Tiger AU is even viable for the
"validate-engine-pre-AFSL-without-funding" workflow:

1. **Does Tiger AU offer paper trading on the AU entity?**
   Global Tiger (Singapore / global) has historically offered virtual
   accounts. The AU subsidiary may not — it's a separate licensed
   entity and offerings can differ.

2. **Is there a time limit on paper accounts?**
   Saxo Simulation expires after 20 days. If Tiger AU paper has a
   similar expiry, the multi-year evidence narrative is dead. Need
   confirmed indefinite duration.

3. **What's the starting balance, and can it be reset / topped up?**
   We want at minimum AUD 250k to match current IBKR; ideally AUD 1M
   so we can validate at the assumed wholesale entry size. A fixed
   USD 100k start kills the scale story.

4. **Does the OpenAPI work on AU paper accounts?**
   Some brokers gate API access behind live-account funding even
   though the paper-account UI works. We need REST + WebSocket access
   from the `tigeropen` Python SDK against an AU paper account
   specifically.

5. **Can paper accounts trade US ETFs (SOXX, SMH, IVV, QQQ)?**
   The engine universe is ~40% US-listed. If AU paper is restricted
   to ASX-only, the backtest doesn't translate to live.

## Important (affects the fee model)

These don't kill the deal but determine whether the engine's
backtest results are honest:

6. **Confirm AU equity / ETF commission**
   Provisional: AUD 2.99 minimum + 0.029% rate.
   Verify: is the minimum AUD 2.99 or higher? Is the rate 0.029% or
   tiered? Is there a cap?

7. **Confirm US equity / ETF commission**
   Provisional: USD 0.99 minimum + USD 0.0049/share.
   Verify: latest published rate, whether there's a per-share cap,
   whether ETF orders get the same treatment as equities.

8. **Confirm FX spread on AUD↔USD conversions**
   Provisional: ~50 pip retail.
   Verify: what's the actual mid-market markup? Is there a flat fee
   per conversion or just the spread?

9. **Any inactivity / monthly minimum fees?**
   IBKR Pro AU has none. Saxo Classic has none below threshold.
   Confirm Tiger AU's structure.

10. **Tax-report support for AU CGT**
    Tiger AU should generate annualised tax statements compatible
    with AU CGT calc (proceeds, cost base, dates, FX rates if any).
    Confirm format matches what we'd need to import into the engine's
    lot book.

## Useful (for the API port estimation)

11. **`tigeropen` Python SDK version and maintenance status**
    Check the GitHub repo for last-commit date, open issue count,
    whether it's actively supported. A dead SDK is a hidden cost.

12. **WebSocket subscription model**
    Per-symbol limits? Rate limits on order placement? Different from
    REST limits?

13. **Order types supported**
    At minimum we need MarketOrder and LimitOrder. The current
    `ibkr_paper_exec.py` uses MarketOrder. If Tiger AU restricts to
    LimitOrder only, we'd need to bake in price discovery.

14. **Order status / fill notification mechanism**
    IBKR uses event callbacks via ib_insync; Saxo uses WebSocket
    streams. Tiger's pattern matters for the `paper_exec` port.

## Where to look / who to ask

- Tiger AU website → "Open API" or "TigerTrade Pro" section
- `tigeropen` GitHub repo README (https://github.com/tigerfintech — verify URL)
- AU support email: `support_au@itigerup.com` (verify this pattern is
  still current; may have moved to a contact form)
- ASIC's professional investor registry to confirm licence scope

## If everything checks out

Estimated work to port:
- `saxo_paper_exec.py` style mirror against `tigeropen` SDK: 1-2 days
- New contract-builder for Tiger ticker format
- Cost-model already in place (this profile)
- Fills-log writer can reuse existing JSONL schema

## If anything fails verification

Top of list: confirm whether Tiger AU offers indefinite paper, then
asset coverage. If either fails, fall back to:

1. Stay with IBKR $250k paper for validation
2. Reconsider funding IBKR to lift cap (the cost of $50k tied up
   for 12 months is ~AUD 1.5k in lost RBA-cash-rate interest)
3. Webull AU paper as the next research target (less mature API but
   free + AU-listed entity)
