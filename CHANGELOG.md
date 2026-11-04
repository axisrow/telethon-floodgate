# Changelog

## Unreleased

- `try_acquire(..., slots=n)` with `n` above a bucket's `max_calls` (for
  example a 2-message album against the 1-slot `send:user` peer bucket, or
  `slots=2` in the 1-operation `dialogs` category) no longer raises
  `ValueError` — the documented "never sleeps, never raises" contract holds
  (#7). Such a request can never be admitted, so the offending bucket answers
  with a state-independent full-window cadence defer that repeats until the
  caller splits the request, and the refusal records nothing in any bucket
  (the two-phase peek/commit composition is unchanged). Satisfiable `slots`
  requests keep the atomic wait-for-the-whole-request semantics, the wider
  sustained tier still wins the combined defer, and `record_many` keeps
  rejecting oversized commits loudly — committing a reservation no window
  admitted is a programming error, not a rate event.

- `UsernameResolveRateLimitedError` and `UsernameResolveFloodWaitDeferredError`
  are now `TelegramRateLimitedError` subclasses instead of bare `RuntimeError`
  siblings (#11). Backwards compatible: existing `except UsernameResolve*`
  handlers keep working, historical message texts and attributes
  (`retry_after_sec`, `retry_after_seconds`, `next_available_at`,
  `run_after_with_buffer`, `wait_seconds`) are unchanged — and the shared
  consumer handler `except TelegramRateLimitedError: operation unavailable,
  move on` now covers resolve refusals too. `TelegramRateLimitedError` moved to
  a private `_errors` module to avoid a circular import; the public import
  paths (`rate_limit_gate`, package root) are re-exports and unchanged.
- `TelegramRateLimitGate.try_acquire(..., peer=...)` no longer records a
  phantom per-peer slot when the account-wide category refuses (#8): the
  peer+category composition is now two-phase — both windows are peeked
  without recording and committed only when both admit, so a deferral
  reserves nothing anywhere. Previously every category-refused retry burned a
  peer slot, shifting the real send by another peer window during adaptive
  backoff. Peer-first defer ordering (a peer refusal leaves the category
  untouched) and `slots=n` all-or-nothing semantics are unchanged;
  `ResolveRateLimiter` exposes the halves as `peek_many` / `record_many`.
- `scripts/calibrate_send_limits.py`: a point cut short by the
  `--max-messages`/`--max-seconds` budget no longer counts as a flood-free
  interval (#9) — with the old clean-filter (only `flooded`/`sent > 0`
  checked) the truncated last point could become `safe_interval` and the
  report would recommend `window_sec = interval × 1.25` for an interval
  validated by a single send. Points now carry a `truncated` flag in the
  report, budget-cut points stay out of the flood-free verdict, and when no
  interval held its full send count the recommendation names the budget (not
  floods) as the reason and advises against lowering the default. Note: with
  default budgets (50 messages vs 7×8 points) the final point is always
  budget-truncated — pass `--max-messages 56` if you want the full ladder
  measured.
- Unknown operation tags falling back to the `default` category now log one
  `logger.warning` per tag — on the first fallback only (#10). A new consumer
  tag (`telegram_send_poll`) or a typo (`telegram_send_mesage`) used to run
  through the permissive 1000/min default bucket with no signal at all; now
  the first call names the tag in the logs. The fallback itself is unchanged
  (still returns `default`), so behavior stays backward-compatible.
- `FloodCircuitBreaker.record_flood()` releases the half-open probe's claim
  when the trial settles as a flood (`record_success` already did, #6): the
  settled stamp no longer lingers in `_probe_in_flight` until the next grant
  happens to overwrite it. Closes the last path that leaves a claim behind
  after its trial has resolved. New regression tests pin the issue's
  cancellation trigger (a trial cancelled mid-await recovering through the
  one-cooldown self-heal from 0.1.5, #14) and the claim release; a trial
  dying on a non-flood error stays covered by the existing ghost-probe test.

## 0.2.0 (2026-11-05)

- `ResolveRateLimiter` sustained tier is **on by default**: 60 calls / 3600s
  alongside the 20/60s burst. The tier that was opt-in never turned itself
  on, and a second deep FLOOD_WAIT (18h; tg_content_factory 2026-11-04) grew
  out of sustained volume exactly like the 13.8h one on 2026-10-06 (#26).
  The 0.1.x burst-only behaviour stays reachable via the explicit
  `sustained_max_calls=None, sustained_window_sec=None` pair.
- New `tighten(phone, *, factor, duration_sec)` (#26): after a Telegram-side
  signal (rate limit / flood) on a phone, callers halve that phone's
  effective sustained budget for a few hours instead of waiting the signal
  out at full quota — Telegram escalates in steps, and full-quota waits
  invite the next rung. Invalid arguments raise `ValueError`; without a
  sustained tier `tighten` is a no-op returning `None`.
- New `snapshot_state()` / `restore_state()` (#26): per-phone window contents
  as wall-clock stamps, so a restarted daemon carries its sustained volume
  across the restart (monotonic stamps die with the process; wall-clock ones
  replay against the next boot's clock). Merges into existing state.

## 0.1.5 (2026-10-24)

- `FloodCircuitBreaker.check()`: a HALF_OPEN-busy refusal (another coroutine's
  trial call still in flight) now reports an honest short retry hint
  (`HALF_OPEN_RETRY_HINT_SECONDS`, default 2.0, capped by `cooldown_seconds`)
  instead of the flat full cooldown — the in-flight trial typically resolves
  within seconds, and callers honoring `retry_after` used to idle out a
  suspension that was already lifting (#13).
- `FloodCircuitBreaker.check()` self-heals a wedged half-open probe (#14): a
  granted call that reports neither `record_flood()` nor `record_success()`
  (unexpected exception, cancellation, a wrapper bug) used to hold the
  (operation, phone) pair in HALF_OPEN forever — every later `check()` refused
  with a flat cooldown, and only `reset()` or a restart recovered it. A probe
  cannot legitimately outlive the suspension window that granted it, so
  `check()` now re-claims a trial still in flight longer than one cooldown.
  The exactly-once reporting contract (settle each granted trial with exactly
  one `record_flood`/`record_success`, ideally from a `finally` around the
  Telegram call) is documented on `_probe_in_flight`.
- `FloodCircuitBreaker` no longer depends on pybreaker (#15): the three-state
  machine (closed/open/half_open) is explicit — a fail counter and a deadline
  dict per (operation, phone) — with the same public API (`check` /
  `record_flood` / `record_success` / `reset`), thresholds and cooldown. This
  removes the pybreaker-shaped integration (the failure-counting probe trick,
  the listener bridge, the parallel `_open_until` bookkeeping) that was the
  hardest-to-follow part of the codebase; semantics proven in the telegram-mcp
  port (axisrow/telegram-mcp#28), including the #13 retry hint and the #14
  self-heal above. `pybreaker` is dropped from the dependencies — the package
  needs only telethon and pydantic.

## 0.1.4 (2026-10-07)

Review-driven correctness fixes; no API changes.

- `reset(category=...)` now honours its scope when no phone is given too: it
  clears only that category's peer buckets (it used to wipe every account's
  and every category's buckets — the opposite of what 0.1.3 documented), and
  unknown/no-op categories (`resolve`, `reaction`, typos) are a no-op instead
  of `KeyError` on the consumer's unblock path.
- `reset(phone, category="send_peer")` maps onto `send`: the category
  `TelegramPeerRateLimitedError` carries now clears the refusing bucket via
  the endorsed unblock call instead of silently matching nothing.
- Peer specs validate `window_sec` at gate construction (like category
  specs): a degenerate window raises there instead of from inside
  `try_acquire` mid-flight. Non-finite (NaN/inf) windows are rejected for
  burst and sustained windows alike — a NaN window silently disabled the
  limiter.
- `flood_wait_remaining_seconds` boundary note: a fractional remainder just
  above the transient threshold (the 60–61s band) classifies as blocking,
  consistent with the ≤60s policy (truncation used to call it transient).
- `FloodCircuitBreaker`: one cooldown per trip — `record_flood` no longer
  extends a running cooldown (late reports from calls already in flight keep
  the original trial time; a flood after the deadline expired starts a fresh
  cooldown), and `record_success` lifts a running suspension unconditionally:
  a stale mid-trial report can re-trip the breaker, and the trial's success
  must still close the pair.
- `ResolveRateLimiter.try_acquire_many` raises the same `ValueError` as the
  burst check when `slots` exceeds `sustained_max_calls` (it used to raise
  `IndexError` from an empty sustained window).
- `ResolveRateLimiter` rejects `window_sec <= 0` with `ValueError` — a
  non-positive window silently disabled the limiter.
- `flood_wait_remaining_seconds` ceils fractional remaining time: a flood
  expiring in 0.6s now classifies as transient (remaining=1) instead of
  collapsing to 0 — neither transient nor blocking.
- `run_with_flood_wait_retry` accumulates the per-retry cost (wait + buffer)
  against the transient budget, so total actual sleep can no longer exceed
  `transient_wait_budget_sec`.
- `handle_flood_wait` supports synchronous `report_flood` hooks (the result
  is awaited only when awaitable) — a sync hook used to crash flood handling
  with `TypeError`.
- README: `sleep=` is documented for the gate helpers that actually accept it
  (`acquire`, `wrap_messages_iterator`); the `run_with_flood_wait*` helpers
  always use `asyncio.sleep`.

## 0.1.3 (2026-10-07)

Adaptive flood backoff wired end-to-end, gate observability, jitter everywhere.

- `handle_flood_wait`, `run_with_flood_wait` and `run_with_flood_wait_retry`
  accept `gate=` (structural `FloodReportingGate` protocol, exported): every
  handled FLOOD_WAIT is reported to the gate via `note_flood(phone, seconds)`,
  closing the adaptive-backoff loop — the gate only needs to be passed once
  at the call site. `pool.report_flood` semantics unchanged; if you wired a
  manual `pool.report_flood` hook to call `note_flood`, REMOVE that hook when
  passing `gate=` — keeping both double-counts events. The gate reports are
  a no-op unless the gate was constructed with `flood_backoff=True`.
- All category and peer specs ship non-zero default defer jitter: every spec
  without an explicit `jitter_sec` derives 5% of its window (3s on 60s
  windows, 1.5s on history's 30s, 15s on channel_lifecycle's 300s) through
  the single limiter mechanism; the tight 1.1s user peer keeps a deliberate
  0.15s. Consumer overrides derive the same default unless they pass an
  explicit `jitter_sec` — `jitter_sec=0` disables. Starting ratio —
  recalibrate against production samples in one place.

- `TelegramRateLimitGate.note_flood(phone, seconds)` — opt-in per-gate
  adaptive backoff (`flood_backoff=True`): every reported FLOOD_WAIT **above
  the transient threshold (60s)** doubles the defers the gate hands out for
  that phone until the events decay (1h default window, cap 8x, both
  configurable; `flood_backoff_cap < 1` raises instead of silently
  disabling). Routine ≤60s pacing floods — the sweep protocol working as
  designed — do not escalate; `seconds=None` (severity unknown) counts
  conservatively. The multiplier scales category AND per-peer defers; zero
  defers stay zero — backoff slows callers, it invents no refusals.
  Consumers wire it from their own flood reporting (e.g. a
  `pool.report_flood` hook); the layers stay decoupled. Unlike the
  docker-telethon-plus throttler this was ported from, bucket waits are
  scaled too, not just inter-call gaps.
- `TelegramRateLimitGate.snapshot(phone)` — live per-account state:
  per-category usage and specs, per-peer buckets (incl. defer jitter), the
  current flood multiplier and events-in-window. Event-loop-affine; feed
  for dashboards/health endpoints.
- `TelegramRateLimitGate(jitter_func=...)` — deterministic jitter injection
  threading to every category and peer limiter (mirrors `time_func`).
- `acquire()` sleeps a flood-multiplied defer in ≤30s slices
  (`ACQUIRE_SLEEP_CAP_SEC`) and re-checks on every wake, so `reset()` keeps
  working as the unblock escape hatch during long backoffs.
- `reset()` scoping tightened: `reset(phone, category=...)` clears only that
  category's peer buckets, and a category-scoped reset no longer wipes
  per-phone flood events (they have no category dimension).
- `SEND_PEER_USER_SPEC` gains 0.15s defer jitter so retried sends around the
  calibrated 1/1.1s boundary do not fire metronome-precisely (same lockstep
  rationale as `ResolveRateLimiter`'s jitter).
- `ResolveRateLimiter.used(phone)` — burst-window usage for observability;
  read-only (an unknown phone seeds no window entry).

## 0.1.2 (2026-10-07)

Shared async pacing plus the opt-in live-testing harness.

- `TokenBucket`: smooth-refill outgoing cap extracted from tg_messenger,
  retaining its burst, rate opt-out, injectable time/sleep and fair wait queue
- `TelegramRateLimitGate.acquire`: wait and re-acquire one category slot after
  every deferral; cancellation propagates without a new reservation
- `TelegramRateLimitGate.wrap_messages_iterator`: gate actual Telethon history,
  search and ID-fetch page RPCs without modifying the shared client; no changes
  to category defaults, per-peer policy, or the existing retry/error APIs

## 0.1.1 (2026-10-07)

Opt-in live-testing harness + sustained-volume tier for `ResolveRateLimiter`.

- `ResolveRateLimiter` gains optional `sustained_max_calls` /
  `sustained_window_sec` (second sliding window, same deque math) — caps
  accumulated call volume, not just per-minute bursts. Recommended companion
  to the 20/60s burst: 60 calls / 3600s. Production incident
  (tg_content_factory 2026-10-06): a cold collect of 623 channels fired
  20 resolves/min — every 60s burst window green — for 20+ minutes until
  Telegram answered `FLOOD_WAIT_49613`; the burst window alone cannot see
  accumulated volume.
- Both windows are checked first; timestamps are recorded in both only when
  both admit the call — a deferred call burns no slots (unlike composing two
  separate limiter instances).
- Sustained tier is opt-in per instance (`None` = 0.1.x behaviour);
  validation: params must come in a pair, sustained window cannot be
  narrower than the burst window; `reset()` clears both windows.
- `tests_live/` outside `testpaths`: gated live tests (`RUN_FLOODGATE_LIVE_TG=1`
  + `REAL_TG_*` env) — read-only peer-key classification over real dialogs and
  a bounded send test proving the per-peer 1/s bucket paces Saved-Messages
  traffic with no FloodWaitError
- `scripts/calibrate_send_limits.py`: manual raw-boundary probe with fixed
  interval points, hard budgets and auto-stop on the first flood
- offline invariants in CI: marker registration, default-run exclusion,
  no-secret-literals audit, full gate-policy matrix
- live smoke checks surface short flood waits, measure send-start spacing,
  and skip cleanup after a flood; an offline regression covers variable
  response latency and immediate/mid-run floods without contacting Telegram
- empirical quota guardrails: `history` 24 requests / 30s, user-peer sends
  1 / 1.1s, and channel/chat peer sends 16 / 60s, each with documented
  evidence and a conservative margin

## 0.1.0 (2026-08-31)

Initial release — 1:1 extraction of the flood stack from the tg_content_factory
project, plus per-peer send limits.

- `TelegramRateLimitGate`: proactive per-(phone, category) sliding windows
  plus per-peer send buckets `(phone, category, peer)` — independent from the
  category budget, with `peer_key()` extraction from Telethon entities
  (`peer.py`) and `TelegramPeerRateLimitedError`
- `FloodCircuitBreaker`: pybreaker-based suspension per (operation, phone)
- `ResolveRateLimiter`: sliding-window limiter for `auth.resolveUsername`
- flood-wait helpers: `run_with_flood_wait`, `run_with_flood_wait_retry`,
  transient/blocking classification, sleep helpers, `FloodWaitInfo`
