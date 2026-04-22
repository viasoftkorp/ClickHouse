# Parallel Replicas — Presentation Plan

## Context

Working plan for a Google Slides deck on query processing with parallel replicas. Scope agreed with the user:

1. Short intro about current distributed query processing model
2. How parallel replicas fit the current model
3. Some implementation details
4. Moving to sending query plan to replicas instead of query text
   (including "Distributed 2.0" — applying plan serialization to classic distributed too)

**Working style:** go section by section. Don't drop the full deck at once.

**Status:** Section 1 outline drafted, awaiting user feedback before fleshing out. Sections 2–4 have a sketch only.

---

## Artifacts already produced (in `/home/devcrafter/git/ClickHouse/tmp/`)

- `parallel_replicas_report.html` — HTML report of all parallel-replicas PRs 2024-04-21 → 2026-04-21 (237 merged, 161 primary, 13 landmarks). Categorized, with timeline.
  Previously served at `http://192.168.178.29:8765/parallel_replicas_report.html` via `python3 -m http.server 8765` (server stopped on reboot).
- `distributed_query_overview.svg` — SVG diagram of classic distributed execution flow (Client → Initiator → Shards → blocks back). Reuse this for Section 1, Slide 1.1 or 1.2.
- `pr_by_month.md`, `pr_categorized.md` — raw PR listings.

The HTML landmarks set (copy-paste for slides):
`#63151, #63796, #64448, #70171, #72109, #74504, #76427, #79401, #82807, #87541, #88262, #88696, #97517`.

---

## Overall deck outline (agreed at a high level)

| Section | Slides | Purpose |
|---|---|---|
| 1. Current distributed query processing | ~4 | Baseline so audience can follow the rest |
| 2. How parallel replicas fit the model | ~4 | Show it's an extension, not a separate thing |
| 3. Implementation details | ~4–5 | Coordinator, local plan, settings, JOINs, failures |
| 4. Plan serialization + Distributed 2.0 | ~4 | Why, what changes, where we are |
| 5. Closing | 1 | Takeaways |

### Concrete walkthrough idea (user liked it)

Trace the **same one or two queries** through four setups: classic distributed → parallel replicas → plan serialization → distributed 2.0. Weave this through the sections, not all in one place.

- **Query A** (basic): `SELECT country, count() FROM events_dist WHERE ts >= today()-1 GROUP BY country ORDER BY cnt DESC LIMIT 10`
- **Query B** (interesting, exposes GLOBAL IN / JOIN): `SELECT country, count() FROM events_dist WHERE user_id IN (SELECT id FROM users_dist WHERE plan='pro') GROUP BY country`

A comparison table at the end:

| Aspect | Classic | Parallel replicas | + Plan ser. | Distributed 2.0 |
|---|---|---|---|---|
| Fan-out target | one replica per shard | all replicas of one shard | same | shards + replicas |
| Wire payload | SQL text | SQL text + tasks | serialized plan + tasks | serialized plan |
| Parse/analyze on remote | yes | yes | **no** | **no** |
| Coordination | none | mark ranges | mark ranges | both |
| Replica utilization | 1/N | N/N | N/N | N/N |
| Data read per node | full shard | shard/N | shard/N | shard/N |

---

## Section 1 — Current distributed query processing (draft, ~4 slides)

**Goal:** audience leaves knowing the initiator/shard split, what travels over the wire, and what `QueryProcessingStage` does. Nothing else.

### Slide 1.1 — Cluster topology
- Cluster = named set of shards; shard = set of replicas
- Defined in `remote_servers` (or Keeper in Cloud)
- Symmetric: every node knows the topology; any node can be the initiator
- Visual: 3-shard × 2-replica diagram, one node highlighted as "initiator"

### Slide 1.2 — Roles & query kinds
- Initiator: parses, analyzes, rewrites, fans out, merges, returns to client
- Shard: receives a secondary query, runs it locally, streams blocks back
- `ClientInfo.query_kind`: `INITIAL_QUERY` on initiator, `SECONDARY_QUERY` on shards
- Visual: 1 initiator + 2 shards, arrows (query out / blocks back). Can reuse `distributed_query_overview.svg`.

### Slide 1.3 — What actually travels on the wire
- `FROM dist_t` is swapped for `FROM local_t` before dispatch
  - Code: `buildQueryTreeDistributed` → `cloneAndReplace` in `src/Storages/StorageDistributed.cpp:927`
  - Then serialized back to SQL by `queryNodeToDistributedSelectQuery` (`src/Planner/Utils.cpp:201`)
- Side-by-side example (use a result from the local test we ran):
  - Original: `SELECT country, total FROM (SELECT user_id AS country, sum(x) AS total FROM dist_t GROUP BY user_id) WHERE total > 1000 ORDER BY total DESC LIMIT 5`
  - Sent to shard: `SELECT __table1.user_id AS country, sum(__table1.x) AS total FROM default.local_t AS __table1 GROUP BY __table1.user_id`
- Also sent: `ClientInfo`, settings, external / scalar data

### Slide 1.4 — Where does work stop on the shard? `QueryProcessingStage`
- Stages: `FetchColumns` / `WithMergeableState` / `WithMergeableStateAfterAggregation` / `Complete`
- Initiator picks the stage per query; shards execute up to that point
- Shards return partial aggregation states, not final values (unless stage = `Complete`)
- Final merge / sort / limit / distinct on initiator
- Visual: pipeline timeline — shard bars end early; initiator bar finishes the job

### Optional / backup slide
- Replica selection (load_balancing, hedged requests, errors_count). Orthogonal to the story — only show if asked.

### Open questions for next session (before fleshing out)
1. Is 4 slides the right budget, or go shorter (2–3)?
2. Write speaker copy per slide, or stay at outline level?
3. Reuse the big SVG or make smaller per-slide visuals?

---

## Section 2 — How parallel replicas fit (sketch)

- Motivation: classic sharding scales across disjoint data, replicas idle for reads; parallel replicas = second parallelism axis within one shard
- Slot in existing model: same initiator/secondary mechanics, different `ReadFromRemote` variant (`ReadFromParallelRemoteReplicas`)
- Secondary queries go to replicas of one shard, all running the same SQL, reading disjoint mark ranges
- Coordinator + task protocol: one replica (usually the initiator) hands out mark-range tasks; others pull. Deduplicates parts across replicas. Modes: Default / ReadInOrder / ReadInReverseOrder.
- Entry points: `StorageDistributed::read` decides via `canUseTaskBasedParallelReplicas`; `buildQueryTreeForShard(..., /*parallel_replicas*/ true)`.

---

## Section 3 — Implementation details (sketch)

- Local plan for local replica (#70171): initiator often is a participating replica — avoid sending SQL to self.
- Settings knobs: `max_parallel_replicas`, `parallel_replicas_for_non_replicated_merge_tree`, `cluster_for_parallel_replicas`, `parallel_replicas_mode`, `automatic_parallel_replicas_mode`, `automatic_parallel_replicas_min_bytes_per_replica`, `parallel_replicas_filter_pushdown`.
- Index analysis on coordinator (#72109): one-place pruning; replicas only execute assigned ranges.
- JOINs are the hard part: GLOBAL JOIN/IN materialization + non-leftmost join fixes (#71162, #72393, #72510, #73451, #86895, #87178).
- Failure modes & telemetry: `ALL_CONNECTION_TRIES_FAILED`; stale `_shard_num` (#101208); `system.query_log` linked by `initial_query_id`; ProfileEvents.

---

## Section 4 — Plan serialization / Distributed 2.0 (sketch)

- Why the SQL-text channel hurts: lossy (alias / analyzer details), re-parse on each replica, version-sensitive.
- What "send the plan" means: serialize `QueryPlan` post-analysis, replicas deserialize → pipeline → execute. New packet types, version-gated.
- Benefits for parallel replicas: coordinator and replicas share the exact same plan; simpler pushdown; enables richer coordination.
- Distributed 2.0: same idea for cross-shard fan-out; composes with parallel replicas hierarchically; unified plan graph initiator → replicas.
- Status: landing step by step — `#88696` "serialized query plan (part 1)".

---

## Section 5 — Closing (sketch)

- Takeaway: parallel replicas = second parallelism axis on the existing initiator/shard model
- Plan serialization = natural next step for correctness + performance
- Distributed 2.0 = unification, not a separate track

---

## How to resume next session

Paste something like:

> Continue the parallel-replicas talk plan at `/home/devcrafter/.claude/plans/parallel-replicas-talk.md`. We finished Section 1 outline — I want to [answer the 3 open questions / move to Section 2 / flesh out Slide 1.3].

I'll read the file and pick up from the "Status" line at the top.
