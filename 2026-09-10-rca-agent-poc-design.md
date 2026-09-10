# AI Incident RCA Agent — POC Design

**Status:** Draft for review
**Date:** 2026-09-10
**Scope:** Application failures for one application, one repository. Batch jobs are out of scope.

---

## 1. Problem

A Splunk alert fires when an application throws an unhandled exception. Today it lands
in a shared Outlook mailbox. A developer then manually correlates application logs,
source code, and release history to produce an explanation of what happened and why.

This is repetitive, slow, and the quality depends on who is on call.

## 2. Goal

Automate the evidence-gathering and hypothesis-forming stage. The system produces a
ranked, evidence-cited root cause analysis in a purpose-built React UI, and lets a
developer ask follow-up questions against the same investigation context.

**Non-goal:** the system never takes remediating action. It is advisory only for the
POC. Auto-remediation is a later phase and requires its own design and approvals.

## 3. Constraints

| Constraint | Value |
|---|---|
| LLM provider | BroadGPT (wrapper over ChatGPT), accessed via a client we write ourselves |
| Tool calling | **Unconfirmed.** Must be verified before build starts. Fallback is textual ReAct. |
| Log access | Splunk Cloud. Alert payload available now; REST search token pending approval. |
| Code access | GitLab, one repository |
| Database access | None |
| Deploy/CI history | None |
| Backend | Python |
| Frontend | React |
| Orchestration | LangGraph, ReAct-style tool loop |

### 3.1 Declared limitations

These follow directly from the constraints and must appear in the product UI, not just
in this document:

- Without deploy history, the system cannot attribute a failure to a specific release.
- Without database access, it cannot distinguish a data-state problem from a code
  defect. Any hypothesis depending on DB state is capped at `possible`.
- Until the Splunk REST token is approved, the log analyst operates on the alert
  payload alone and cannot run follow-up searches.

## 4. Architecture

Six layers. Determinism is preferred everywhere it is possible; LLM calls are reserved
for judgement.

### 4.1 Sources
Splunk Cloud (REST + SPL), Outlook (Microsoft Graph), GitLab (REST v4), BroadGPT.

### 4.2 Ingestion
Two interchangeable triggers normalising to a single `AlertEvent`:

- **Splunk webhook** — preferred. Structured JSON, immediate. Requires network path
  from Splunk Cloud to the internal endpoint.
- **Mailbox poll** — fallback. ARQ cron every 5 minutes reading via Graph API.
  Requires no firewall change.

The adapter choice is a config flag. No downstream component knows which is active.

The webhook handler validates, persists an `Incident` row, enqueues, and returns 202
in under 50ms. Investigations run for minutes, so all real work happens in a worker
process.

### 4.3 Preprocessing

**Normalisation.** HTML alert mail to text; encoding repair; parse into `RawAlert`.

**Two-stage triage.** Most alert mail is informational. Stage L1 applies deterministic
keyword and pattern rules to subject lines and discards known-informational mail at
zero cost. Stage L2 sends the surviving subjects to the LLM in a single batched call
returning a JSON classification. Only mail classified as a failure has its full body
fetched.

No machine-learning framework is used. There is no labelled corpus and no training
loop. If a learned classifier is wanted later, scikit-learn with TF-IDF over
accumulated labels is the appropriate tool.

**Redaction.** Every artefact is scrubbed before persistence and before any LLM call.
Regex layer for exactly specifiable identifiers (account numbers, CUSIP/ISIN, emails,
IP addresses); Presidio added only if named-entity detection is required by security
review. Redaction maps to stable tokens (`ACCT_A1`, `ACCT_A2`) so the model can reason
about repeated identifiers without seeing them.

This is a first-class component with its own fixture suite, because it is what the
security conversation will centre on.

**Structured extraction.** Deterministic extraction first (timestamps via dateutil,
hostnames and environment via regex). Only residual free text goes to the LLM, which
returns an `AlertEnvelope`: timestamp, application, server, environment, alert type,
summary.

### 4.4 Agent runtime

A LangGraph `StateGraph`. Nodes are plain async Python functions; no LangChain chat
model abstraction is used, because we supply our own LLM client.

**Recall (deterministic).** Before investigation, two lookups: exact fingerprint match,
and vector similarity over prior RCAs. A fingerprint hit short-circuits to the cached
result. A similarity hit is injected as prior context for the supervisor.

**Supervisor.** Owns the investigation plan. Reads findings only, never raw evidence,
which keeps its context near-constant regardless of incident size. Dispatches
specialists, evaluates results, decides whether to continue.

**Log analyst.** Tool: `splunk_search`. Reconstructs the timeline — first occurrence,
frequency, correlated errors in the same thread or trace.

**Code analyst.** Tools: `gitlab_get_file`, `gitlab_search`, `gitlab_commits`. Maps
stack frames to source. **All file fetches are pinned to the deployed version tag**,
extracted from the log banner or manifest — never repository HEAD. If the deployed
version cannot be determined, the report says so rather than assuming.

**Synthesis.** Produces ranked hypotheses with confidence bands and evidence citations.
Ranked, not singular — real incidents often have two plausible causes.

**Critic.** Receives only the claims and their cited evidence, never the reasoning
trace. Its question is structural: does every claim carry a citation, and does each
citation actually contain what is claimed. A blind checker with a narrow question is
worth more than a smart one with the full story.

### 4.5 Context transfer

The rule that makes multi-agent viable: **agents never pass transcripts, and raw
evidence never travels between them.**

```
Finding
  id, agent, claim, evidence_ids[], support_type, confidence, supersedes?
```

A claim is one sentence. Everything backing it is a reference. Raw artefacts live in
the evidence store, addressed by `evidence_id`, and are hydrated on demand through a
budget-counted `get_evidence` call.

Context is assembled in Python per call. There is no running conversation. Who sees
what:

| Agent | Sees |
|---|---|
| Supervisor | Claims only |
| Specialists | Evidence they fetched, plus others' claims for orientation |
| Synthesis | All claims; hydrates evidence by ID on demand |
| Critic | Claims and cited evidence only |

Log volume is reduced deterministically before it reaches any agent: Python aggregates
occurrence counts, time buckets, and distinct message templates, and passes at most K
representative samples. This single decision is the difference between an incident
costing cents and costing dollars.

### 4.6 Confidence model

Confidence is **derived in Python from evidence shape**, never self-reported by the
model. Self-reported confidence tracks prompt phrasing rather than evidence quality.

Each finding carries a support type:

- `direct` — visible in the artefacts
- `correlational` — timing or frequency supports it
- `inferential` — the code permits it; nothing confirms it happened

Reported bands are `likely`, `possible`, `insufficient_evidence`. No percentages —
they imply a precision we cannot back. `inferential`-only never reaches `likely`.

**Unknowns register.** Every hypothesis declares what would confirm or refute it.
Anything the system cannot see is recorded rather than glossed. This is also the
artefact that makes the phase-2 access request concrete.

`insufficient_evidence` is a legitimate terminal state, not a failure.

### 4.7 Budgets and termination

Every agent has a tool-call cap and a wall-clock timeout. The supervisor has a cap on
total specialist invocations. Termination on any of:

1. Top hypothesis reaches `likely`
2. Every remaining unknown is one no available tool can check
3. Budget exhausted — produces a partial report, not an error

### 4.8 Persistence

One PostgreSQL instance. No separate vector database, no object store.

| Table | Purpose |
|---|---|
| `incidents` | Alert envelope, fingerprint, status |
| `evidence` | Redacted artefacts as JSONB, 30-day retention |
| `findings` | Claims with evidence references |
| `rca_reports` | Final output, retained indefinitely |
| `rca_embeddings` | pgvector column over RCA summary text |
| `chat_messages` | Developer follow-up conversation per incident |
| `llm_traces` | Every LLM call: prompt, response, tokens, latency |
| LangGraph checkpoints | Managed by `AsyncPostgresSaver` |

**Chat history and investigation context.** `thread_id = incident_id`. Every node
transition checkpoints automatically, giving resumability, replay, and history from one
mechanism.

Graph state and chat history are deliberately separate. Graph state holds findings,
evidence references, budgets, and the unknowns register. `chat_messages` holds the
developer's follow-up turns. Conflating them means every follow-up drags the whole
investigation transcript into context and quality degrades within a few turns.

A follow-up resumes the same thread: load checkpoint, append the human turn, run a
`followup` node with a fresh smaller budget that can re-read evidence and call tools.
Context assembly is unchanged — findings, last N chat turns, evidence hydrated by ID.

### 4.9 Delivery

FastAPI with `sse-starlette` streaming investigation progress. React with TanStack
Query.

Because every claim carries evidence IDs, each RCA statement renders with its
supporting log line or code snippet, expandable inline. A developer who doubts the
conclusion verifies it in one click. This is the same mechanism the Critic uses — one
design buys both hallucination defence and the transparency that drives adoption.

## 5. LLM client

Written in-house, not sourced from a vendor SDK. Requirements:

- `httpx.AsyncClient` with connection pooling and explicit timeouts
- `tenacity` retry with exponential backoff on 429 and 5xx
- Pydantic request/response models so malformed replies fail at the boundary
- `tiktoken` pre-flight token counting, so budgets are enforced before sending
- Every call written to `llm_traces`

**Tool calling has two modes behind one interface.** Native mode uses the OpenAI
`tools` parameter and parses `tool_calls`. ReAct mode prompts for a JSON action object,
repairs it with `json-repair`, and validates against a Pydantic schema, retrying once.
The graph never knows which mode is active.

## 6. Technology

| Concern | Choice |
|---|---|
| Orchestration | langgraph, langgraph-checkpoint-postgres |
| HTTP | httpx |
| Retry | tenacity |
| Models/validation | pydantic v2, pydantic-settings |
| LLM JSON repair | json-repair |
| Token counting | tiktoken |
| Mail | msgraph-sdk |
| Repo | python-gitlab |
| HTML/text | beautifulsoup4, lxml, ftfy |
| Fuzzy matching | rapidfuzz |
| Redaction | re, optionally presidio-analyzer |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) unless BroadGPT exposes an endpoint |
| DB | PostgreSQL + pgvector, SQLAlchemy 2.x async, alembic |
| Queue | ARQ + Redis |
| API | FastAPI, uvicorn, sse-starlette |
| Logging | structlog |
| Tracing | langfuse |
| Testing | pytest, pytest-asyncio, respx, testcontainers |
| Frontend | React, TypeScript, TanStack Query |

**Rejected:** TensorFlow (no training loop, no corpus), splunk-sdk (aging, synchronous;
httpx is less code), separate vector DB (pgvector avoids a second datastore),
LangChain chat models (unnecessary abstraction over our own client).

## 7. Testing

The system must be developable without production access. Record real incidents once —
alert payload, Splunk responses, GitLab responses — as fixtures. `respx` replays them.
The entire agent suite then runs offline and deterministically.

This is what lets prompt iteration take minutes instead of waiting for prod to break.

Layers: unit tests for deterministic components (redaction, fingerprinting, stack-trace
parsing, log aggregation, confidence derivation); replay tests for graph behaviour with
a stubbed LLM returning canned responses; contract tests for the LLM client against a
mocked BroadGPT.

## 8. Evaluation

The POC will be judged on whether it is right. That needs a number, not an anecdote.

- Every RCA carries thumbs up/down and a free-text "actual cause" field.
- Build a labelled set of 20–30 historical incidents with known causes; run the system
  against them and measure top-hypothesis accuracy and top-3 accuracy.
- Track cost per incident and cache hit rate.
- Track the rate of `insufficient_evidence` and which unknowns cause it — that list is
  the phase-2 access business case.

## 9. Open questions

1. **Does BroadGPT support native tool calling?** Blocks the client design. Highest
   priority.
2. **Does BroadGPT expose an embeddings endpoint?** Decides whether
   sentence-transformers is needed.
3. **Splunk REST token approval timeline.**
4. **Is the Splunk Cloud to internal network path available?** Decides webhook vs
   mailbox.
5. **OSS dependency approval for langgraph.** Fallback is a hand-written state machine
   of roughly 400 lines.

## 10. Phasing

**Phase 1 — Ingestion and triage.** Adapters, normalisation, two-stage triage,
redaction, extraction, persistence. Deliverable: alerts reliably become structured,
redacted `AlertEnvelope` records. No agents.

**Phase 2 — LLM client and evidence collection.** Client with both tool-calling modes,
Splunk and GitLab collectors, evidence store, fingerprint cache.

**Phase 3 — Agent graph.** Supervisor, specialists, synthesis, critic, budgets,
confidence derivation, checkpointing.

**Phase 4 — UI and recall.** React UI with evidence drill-down, SSE progress, follow-up
chat, pgvector recall, feedback capture.

**Later.** Batch jobs; DB and deploy-history access; auto-remediation.
