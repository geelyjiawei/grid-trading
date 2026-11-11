# Manual Position Compatibility

## Approved Intent

The user closes only their old/manual inventory, never intentionally the grid's
inventory. Support old same-direction positions and subsequent manual increases
and decreases without recreating manually closed exposure. Keep Rust execution,
quantity/ownership/deduplication checks, and durable-before-submission writes.

## Ownership Contract

- Preserve the immutable startup baseline and all grid inventory events.
- Expected exchange net = baseline + confirmed external execution delta + grid net.
- An external execution is identified by exchange/account context, symbol, exact
  exchange order ID, trade ID, signed quantity and execution time. A position
  difference alone is never evidence of a manual trade.
- Replay complete official account execution history from strategy creation on
  reconciliation. Exclude and verify owned executions using durable order/trade
  identities. Reject unknown/pending owned executions, conflicting duplicates,
  incomplete/truncated history, ambiguous creation-time fills, and reversed or
  overclosed external inventory. Never rewrite the baseline to make totals fit.
- Manual inventory stays in the grid direction (neutral grids retain their
  existing restriction). Same-millisecond opposite manual fills with no proven
  order are rejected conservatively.
- Grid orders, quantities, pairs, fees and realized/unrealized profit do not
  absorb manual activity. Funding uses actual receipts and complete account
  execution evidence, never an estimated zero or the whole account charge.
- Stop still cancels only owned orders. Risk exit closes only grid net. If an
  external close exceeds the manual budget, do not repair it with new exposure;
  block further submissions until evidence is reconciled or the grid is stopped.

## Integration And Limits

Normal fill-to-replacement execution must not acquire a new REST dependency.
Periodic position mismatches invoke bounded read-only history reconciliation,
re-read the authoritative position, and commit external evidence before allowing
further placements. Persist a reconciliation blocker across recovery so a fast
event cannot bypass an already-observed external inventory conflict.

Exchange-side manual orders cannot be intercepted. Between an external trade and
its observation the application cannot promise physical isolation in a netted
account. Cross margin, shared leverage/balance, forced liquidations, opposing
positions and subaccount migration are not solved by virtual ownership.

Old files deserialize with empty external history. Legacy boundaries that cannot
be proven remain blocked rather than silently adopted. Do not modify production
positions/orders, build on production, or deploy this new feature in this task.

## Acceptance

The 0.5 manual short + grid short example, after manual buy 0.2, must produce
manual short 0.3 plus unchanged grid short. Exercise full close, add/reclose,
partial fills, duplicates/conflicts, unknown owned execution, overclose/opposite,
history error/pagination caps, recovery and storage failure, grid-only risk exit,
funding ownership, and no extra history reads on ordinary fast replacements.
