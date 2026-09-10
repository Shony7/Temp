# RCA Agent Phase 1: Ingestion and Triage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn raw Splunk alerts — arriving by webhook or Outlook mailbox — into structured, redacted `AlertEnvelope` records in PostgreSQL, discarding informational noise in two triage stages.

**Architecture:** A FastAPI app exposes a webhook; an ARQ cron job polls a mailbox as an interchangeable alternative. Both normalise to one `RawAlert`. A deterministic pipeline then normalises text, redacts identifiers, applies L1 rule-based triage, escalates survivors to a batched L2 LLM classification, and extracts a structured envelope. No agents in this phase.

**Tech Stack:** Python 3.11+, FastAPI, ARQ, SQLAlchemy 2.x async, alembic, Pydantic v2, beautifulsoup4, lxml, ftfy, rapidfuzz, json-repair, msgraph-sdk, pytest, pytest-asyncio, testcontainers

**Spec:** `docs/superpowers/specs/2026-09-10-rca-agent-poc-design.md`

## Global Constraints

- Python 3.11 minimum. Type hints on all public functions.
- Pydantic v2 syntax only (`model_validate`, `model_dump` — never v1 `parse_obj`/`dict`).
- SQLAlchemy 2.x async style (`async_sessionmaker`, `AsyncSession`).
- **No text may be persisted or sent to an LLM before passing through `redact()`.** This is the single hardest rule in the project.
- No machine-learning framework. TensorFlow and PyTorch are not dependencies.
- All I/O is async. No synchronous HTTP or DB calls in request or worker paths.
- Every module under `src/rca/`. Every test mirrors the path under `tests/`.
- Conventional commit messages (`feat:`, `test:`, `chore:`).

---

### Task 1: Project scaffold, configuration, and database schema

**Files:**
- Create: `pyproject.toml`
- Create: `src/rca/__init__.py`
- Create: `src/rca/config.py`
- Create: `src/rca/db/__init__.py`
- Create: `src/rca/db/models.py`
- Create: `src/rca/db/session.py`
- Create: `alembic.ini`, `migrations/env.py`, `migrations/versions/0001_initial.py`
- Test: `tests/test_config.py`, `tests/db/test_models.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Settings` (pydantic-settings), `get_settings() -> Settings`, `Incident` ORM model, `get_session() -> AsyncSession` async context manager

- [ ] **Step 1: Write the failing config test**

```python
# tests/test_config.py
import os
from rca.config import get_settings


def test_settings_read_from_env(monkeypatch):
    monkeypatch.setenv("RCA_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/rca")
    monkeypatch.setenv("RCA_REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("RCA_ALERT_SOURCE", "webhook")
    get_settings.cache_clear()
    s = get_settings()
    assert s.database_url == "postgresql+asyncpg://u:p@localhost/rca"
    assert s.alert_source == "webhook"


def test_alert_source_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv("RCA_ALERT_SOURCE", "carrier_pigeon")
    get_settings.cache_clear()
    try:
        get_settings()
    except Exception as exc:
        assert "alert_source" in str(exc)
    else:
        raise AssertionError("expected validation error")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.config'`

- [ ] **Step 3: Write `pyproject.toml`**

```toml
[project]
name = "rca-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "pydantic>=2.9",
    "pydantic-settings>=2.6",
    "sqlalchemy[asyncio]>=2.0",
    "asyncpg>=0.30",
    "alembic>=1.14",
    "arq>=0.26",
    "httpx>=0.27",
    "tenacity>=9.0",
    "beautifulsoup4>=4.12",
    "lxml>=5.3",
    "ftfy>=6.3",
    "rapidfuzz>=3.10",
    "json-repair>=0.30",
    "structlog>=24.4",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3",
    "pytest-asyncio>=0.24",
    "respx>=0.21",
    "testcontainers[postgres]>=4.8",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["src"]
```

- [ ] **Step 4: Write `src/rca/config.py`**

```python
from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RCA_", extra="ignore")

    database_url: str = "postgresql+asyncpg://rca:rca@localhost/rca"
    redis_url: str = "redis://localhost:6379"
    alert_source: Literal["webhook", "mailbox"] = "webhook"
    mailbox_poll_seconds: int = 300
    l2_batch_size: int = 40


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 5: Run config test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS

- [ ] **Step 6: Write the failing model test**

```python
# tests/db/test_models.py
from datetime import datetime, timezone

from rca.db.models import Incident, IncidentStatus


def test_incident_defaults():
    inc = Incident(
        source="webhook",
        external_id="splunk-123",
        subject="NullPointerException in OrderService",
        received_at=datetime.now(timezone.utc),
    )
    assert inc.status == IncidentStatus.RECEIVED
    assert inc.envelope is None
```

- [ ] **Step 7: Run test to verify it fails**

Run: `pytest tests/db/test_models.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.db.models'`

- [ ] **Step 8: Write `src/rca/db/models.py`**

```python
import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class IncidentStatus(str, enum.Enum):
    RECEIVED = "received"
    DISCARDED_L1 = "discarded_l1"
    DISCARDED_L2 = "discarded_l2"
    TRIAGED = "triaged"
    FAILED = "failed"


class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (UniqueConstraint("source", "external_id", name="uq_incident_source_external"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    source: Mapped[str] = mapped_column(String(16))
    external_id: Mapped[str] = mapped_column(String(255))
    subject: Mapped[str] = mapped_column(Text)
    body_redacted: Mapped[str | None] = mapped_column(Text, default=None)
    envelope: Mapped[dict | None] = mapped_column(JSONB, default=None)
    triage_reason: Mapped[str | None] = mapped_column(Text, default=None)
    status: Mapped[IncidentStatus] = mapped_column(
        Enum(IncidentStatus, name="incident_status"), default=IncidentStatus.RECEIVED
    )
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
```

- [ ] **Step 9: Write `src/rca/db/session.py`**

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from rca.config import get_settings

_engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
_factory = async_sessionmaker(_engine, expire_on_commit=False)


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    async with _factory() as session:
        yield session
```

- [ ] **Step 10: Generate and apply the initial migration**

Run:
```bash
alembic init migrations
# set sqlalchemy.url from rca.config in migrations/env.py, target_metadata = Base.metadata
alembic revision --autogenerate -m "initial incidents table"
alembic upgrade head
```
Expected: `incidents` table exists with the unique constraint.

- [ ] **Step 11: Run all tests**

Run: `pytest -v`
Expected: PASS

- [ ] **Step 12: Commit**

```bash
git add pyproject.toml src/rca tests alembic.ini migrations
git commit -m "feat: project scaffold, settings, and incidents schema"
```

---

### Task 2: Text normalisation

**Files:**
- Create: `src/rca/ingest/__init__.py`
- Create: `src/rca/ingest/normalize.py`
- Test: `tests/ingest/test_normalize.py`

**Interfaces:**
- Consumes: nothing
- Produces: `normalize_text(raw: str) -> str`

- [ ] **Step 1: Write the failing tests**

```python
# tests/ingest/test_normalize.py
from rca.ingest.normalize import normalize_text


def test_strips_html_and_keeps_table_text():
    html = "<html><body><table><tr><td>App</td><td>OrderService</td></tr></table></body></html>"
    assert normalize_text(html) == "App OrderService"


def test_drops_script_and_style_content():
    html = "<div><style>.x{color:red}</style><script>alert(1)</script><p>Real text</p></div>"
    assert normalize_text(html) == "Real text"


def test_repairs_mojibake():
    assert normalize_text("Donâ€™t panic") == "Don't panic"


def test_collapses_whitespace_and_nbsp():
    assert normalize_text("<p>a&nbsp;&nbsp;b\n\n\nc</p>") == "a b c"


def test_plain_text_passes_through():
    assert normalize_text("NullPointerException at line 214") == "NullPointerException at line 214"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/ingest/test_normalize.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.ingest.normalize'`

- [ ] **Step 3: Write `src/rca/ingest/normalize.py`**

```python
import re
import unicodedata

import ftfy
from bs4 import BeautifulSoup

_WHITESPACE = re.compile(r"\s+")
_HTML_HINT = re.compile(r"<[a-zA-Z/][^>]*>")


def normalize_text(raw: str) -> str:
    """Convert raw alert content (HTML or plain) into clean single-spaced text."""
    if not raw:
        return ""

    text = ftfy.fix_text(raw)

    if _HTML_HINT.search(text):
        soup = BeautifulSoup(text, "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ")

    text = unicodedata.normalize("NFKC", text)
    return _WHITESPACE.sub(" ", text).strip()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/ingest/test_normalize.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/rca/ingest tests/ingest
git commit -m "feat: normalise HTML and mojibake in alert text"
```

---

### Task 3: Redaction engine

This is the compliance-critical component. It gets the most thorough test suite in the phase.

**Files:**
- Create: `src/rca/redact/__init__.py`
- Create: `src/rca/redact/engine.py`
- Create: `src/rca/redact/patterns.py`
- Test: `tests/redact/test_engine.py`

**Interfaces:**
- Consumes: nothing
- Produces: `redact(text: str) -> Redaction`, `Redaction` (Pydantic model with `text: str` and `mapping: dict[str, str]` where keys are tokens and values are category names)

- [ ] **Step 1: Write the failing tests**

```python
# tests/redact/test_engine.py
from rca.redact.engine import redact


def test_replaces_email_with_token():
    r = redact("Contact jane.doe@broadridge.com for details")
    assert "jane.doe@broadridge.com" not in r.text
    assert "EMAIL_A1" in r.text
    assert r.mapping["EMAIL_A1"] == "email"


def test_same_value_gets_same_token():
    r = redact("acct 1234567890123456 failed; retry acct 1234567890123456")
    assert r.text.count("ACCT_A1") == 2
    assert "ACCT_A2" not in r.text


def test_different_values_get_different_tokens():
    r = redact("acct 1234567890123456 and acct 6543210987654321")
    assert "ACCT_A1" in r.text
    assert "ACCT_A2" in r.text


def test_redacts_isin():
    r = redact("Trade failed for US0378331005")
    assert "US0378331005" not in r.text
    assert r.mapping["ISIN_A1"] == "isin"


def test_redacts_ipv4():
    r = redact("connection refused to 10.24.55.101:8080")
    assert "10.24.55.101" not in r.text
    assert "IP_A1" in r.text


def test_preserves_stack_trace_structure():
    text = "java.lang.NullPointerException at com.broadridge.order.OrderService.process(OrderService.java:214)"
    r = redact(text)
    assert r.text == text
    assert r.mapping == {}


def test_does_not_redact_line_numbers_or_versions():
    text = "build 4.11.2 failed after 1200 ms"
    assert redact(text).text == text


def test_empty_input():
    r = redact("")
    assert r.text == ""
    assert r.mapping == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/redact/test_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.redact.engine'`

- [ ] **Step 3: Write `src/rca/redact/patterns.py`**

```python
import re

# Order matters: more specific patterns run first so a generic numeric
# pattern cannot swallow an ISIN or an IP address.
PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("email", "EMAIL", re.compile(r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b")),
    ("isin", "ISIN", re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b")),
    ("cusip", "CUSIP", re.compile(r"\b[0-9A-Z]{8}[0-9]\b(?![.\d])")),
    ("ip", "IP", re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")),
    ("account", "ACCT", re.compile(r"\b\d{10,19}\b")),
]
```

- [ ] **Step 4: Write `src/rca/redact/engine.py`**

```python
from pydantic import BaseModel

from rca.redact.patterns import PATTERNS


class Redaction(BaseModel):
    text: str
    mapping: dict[str, str]


def redact(text: str) -> Redaction:
    """Replace sensitive identifiers with stable per-document tokens.

    The same original value always maps to the same token within one call, so a
    model can reason about repeated identifiers without ever seeing them. Original
    values are never returned or stored.
    """
    if not text:
        return Redaction(text="", mapping={})

    mapping: dict[str, str] = {}
    seen: dict[str, str] = {}
    result = text

    for category, prefix, pattern in PATTERNS:
        counter = 0

        def substitute(match: re.Match[str]) -> str:
            nonlocal counter
            original = match.group(0)
            if original in seen:
                return seen[original]
            counter += 1
            token = f"{prefix}_A{counter}"
            seen[original] = token
            mapping[token] = category
            return token

        result = pattern.sub(substitute, result)

    return Redaction(text=result, mapping=mapping)
```

Add `import re` at the top of the file for the `re.Match` type hint.

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/redact/test_engine.py -v`
Expected: PASS

- [ ] **Step 6: Add a real-alert fixture regression test**

```python
# tests/redact/test_engine.py (append)
import pathlib


def test_real_alert_fixture_has_no_leaks():
    raw = pathlib.Path("tests/fixtures/alerts/sample_splunk_alert.txt").read_text()
    r = redact(raw)
    forbidden = pathlib.Path("tests/fixtures/alerts/sample_splunk_alert.secrets.txt").read_text().split()
    for secret in forbidden:
        assert secret not in r.text, f"leaked {secret}"
```

Create `tests/fixtures/alerts/sample_splunk_alert.txt` from a real (already-sanitised-for-git) alert, and `sample_splunk_alert.secrets.txt` listing the whitespace-separated values that must not survive.

- [ ] **Step 7: Run tests to verify they pass**

Run: `pytest tests/redact -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/rca/redact tests/redact tests/fixtures
git commit -m "feat: redaction engine with stable per-document tokens"
```

---

### Task 4: L1 rule-based triage

**Files:**
- Create: `src/rca/triage/__init__.py`
- Create: `src/rca/triage/models.py`
- Create: `src/rca/triage/l1.py`
- Create: `src/rca/triage/rules.py`
- Test: `tests/triage/test_l1.py`

**Interfaces:**
- Consumes: nothing
- Produces: `TriageDecision` (Pydantic: `is_failure: bool`, `stage: Literal["L1","L2"]`, `reason: str`), `l1_classify(subject: str) -> TriageDecision | None` where `None` means undecided and the subject must escalate to L2

- [ ] **Step 1: Write the failing tests**

```python
# tests/triage/test_l1.py
from rca.triage.l1 import l1_classify


def test_informational_subject_is_rejected():
    d = l1_classify("Scheduled report: Daily Volume Summary")
    assert d is not None and d.is_failure is False and d.stage == "L1"


def test_backup_notice_is_rejected():
    d = l1_classify("Nightly backup completed successfully")
    assert d is not None and d.is_failure is False


def test_obvious_failure_is_accepted():
    d = l1_classify("CRITICAL: NullPointerException in OrderService PROD")
    assert d is not None and d.is_failure is True and d.stage == "L1"


def test_ambiguous_subject_escalates():
    assert l1_classify("OrderService status update") is None


def test_near_duplicate_of_informational_is_rejected():
    d = l1_classify("Scheduled Report -- Daily volume summary")
    assert d is not None and d.is_failure is False


def test_empty_subject_escalates():
    assert l1_classify("") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/triage/test_l1.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.triage.l1'`

- [ ] **Step 3: Write `src/rca/triage/models.py`**

```python
from typing import Literal

from pydantic import BaseModel


class TriageDecision(BaseModel):
    is_failure: bool
    stage: Literal["L1", "L2"]
    reason: str
```

- [ ] **Step 4: Write `src/rca/triage/rules.py`**

```python
# Subjects matching these are informational. Tuned from a sample of the shared
# mailbox; extend as new noise appears rather than loosening the patterns.
INFORMATIONAL_PHRASES: list[str] = [
    "scheduled report",
    "daily volume summary",
    "backup completed successfully",
    "maintenance window scheduled",
    "certificate renewal reminder",
    "weekly capacity report",
]

FAILURE_KEYWORDS: list[str] = [
    "exception",
    "critical",
    "fatal",
    "failed to start",
    "stack trace",
    "timeout",
    "connection refused",
    "out of memory",
]

FUZZY_THRESHOLD = 88
```

- [ ] **Step 5: Write `src/rca/triage/l1.py`**

```python
from rapidfuzz import fuzz

from rca.triage.models import TriageDecision
from rca.triage.rules import FAILURE_KEYWORDS, FUZZY_THRESHOLD, INFORMATIONAL_PHRASES


def l1_classify(subject: str) -> TriageDecision | None:
    """Deterministic first-pass triage.

    Returns a decision when a rule fires confidently, or None when the subject is
    ambiguous and must escalate to the batched L2 classifier.
    """
    if not subject or not subject.strip():
        return None

    lowered = subject.lower()

    for phrase in INFORMATIONAL_PHRASES:
        if phrase in lowered or fuzz.partial_ratio(phrase, lowered) >= FUZZY_THRESHOLD:
            return TriageDecision(
                is_failure=False, stage="L1", reason=f"matched informational rule: {phrase}"
            )

    for keyword in FAILURE_KEYWORDS:
        if keyword in lowered:
            return TriageDecision(
                is_failure=True, stage="L1", reason=f"matched failure keyword: {keyword}"
            )

    return None
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/triage/test_l1.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/rca/triage tests/triage
git commit -m "feat: L1 rule-based subject triage with fuzzy matching"
```

---

### Task 5: LLM client protocol and L2 batch classifier

The concrete BroadGPT client is Phase 2. This task defines the protocol the rest of the system codes against, plus a fake for tests.

**Files:**
- Create: `src/rca/llm/__init__.py`
- Create: `src/rca/llm/protocol.py`
- Create: `src/rca/llm/parsing.py`
- Create: `src/rca/triage/l2.py`
- Test: `tests/llm/test_parsing.py`, `tests/triage/test_l2.py`
- Create: `tests/llm/fakes.py`

**Interfaces:**
- Consumes: `TriageDecision` from Task 4
- Produces: `LLMClient` Protocol with `async def complete(self, *, system: str, user: str) -> str`; `parse_json_list(raw: str, item_model: type[M]) -> list[M]`; `async l2_classify_batch(subjects: list[str], client: LLMClient) -> list[TriageDecision]`; `FakeLLMClient(responses: list[str])`

- [ ] **Step 1: Write the failing parsing tests**

```python
# tests/llm/test_parsing.py
import pytest
from pydantic import BaseModel

from rca.llm.parsing import ParseError, parse_json_list


class Item(BaseModel):
    index: int
    is_failure: bool


def test_parses_clean_json():
    items = parse_json_list('[{"index": 0, "is_failure": true}]', Item)
    assert items[0].index == 0


def test_strips_markdown_fences():
    raw = '```json\n[{"index": 0, "is_failure": false}]\n```'
    assert parse_json_list(raw, Item)[0].is_failure is False


def test_repairs_trailing_comma():
    raw = '[{"index": 0, "is_failure": true},]'
    assert len(parse_json_list(raw, Item)) == 1


def test_raises_on_unrecoverable_output():
    with pytest.raises(ParseError):
        parse_json_list("I could not complete that request.", Item)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/llm/test_parsing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.llm.parsing'`

- [ ] **Step 3: Write `src/rca/llm/protocol.py`**

```python
from typing import Protocol


class LLMClient(Protocol):
    async def complete(self, *, system: str, user: str) -> str:
        """Send a single completion request and return the raw text response."""
        ...
```

- [ ] **Step 4: Write `src/rca/llm/parsing.py`**

```python
import json
import re
from typing import TypeVar

import json_repair
from pydantic import BaseModel, ValidationError

M = TypeVar("M", bound=BaseModel)

_FENCE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.MULTILINE)


class ParseError(ValueError):
    """Raised when an LLM response cannot be coerced into the expected shape."""


def parse_json_list(raw: str, item_model: type[M]) -> list[M]:
    cleaned = _FENCE.sub("", raw).strip()

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        data = json_repair.loads(cleaned)

    if not isinstance(data, list):
        raise ParseError(f"expected a JSON list, got {type(data).__name__}")

    try:
        return [item_model.model_validate(item) for item in data]
    except ValidationError as exc:
        raise ParseError(str(exc)) from exc
```

Note: `json_repair.loads` returns an empty string for unrecoverable input, which is not a list, so the `isinstance` check raises `ParseError` as the fourth test expects.

- [ ] **Step 5: Run parsing tests to verify they pass**

Run: `pytest tests/llm/test_parsing.py -v`
Expected: PASS

- [ ] **Step 6: Write `tests/llm/fakes.py`**

```python
class FakeLLMClient:
    """Returns queued responses in order; records the prompts it received."""

    def __init__(self, responses: list[str]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    async def complete(self, *, system: str, user: str) -> str:
        self.calls.append((system, user))
        if not self._responses:
            raise AssertionError("FakeLLMClient ran out of queued responses")
        return self._responses.pop(0)
```

- [ ] **Step 7: Write the failing L2 tests**

```python
# tests/triage/test_l2.py
import pytest

from rca.llm.parsing import ParseError
from rca.triage.l2 import l2_classify_batch
from tests.llm.fakes import FakeLLMClient


async def test_classifies_a_batch():
    client = FakeLLMClient(['[{"index": 0, "is_failure": true, "reason": "stack trace present"},'
                           ' {"index": 1, "is_failure": false, "reason": "status notice"}]'])
    out = await l2_classify_batch(["Order failure", "Service status"], client)
    assert [d.is_failure for d in out] == [True, False]
    assert all(d.stage == "L2" for d in out)


async def test_sends_one_request_for_the_whole_batch():
    client = FakeLLMClient(['[{"index": 0, "is_failure": true, "reason": "x"},'
                           ' {"index": 1, "is_failure": true, "reason": "y"}]'])
    await l2_classify_batch(["a", "b"], client)
    assert len(client.calls) == 1


async def test_empty_input_makes_no_call():
    client = FakeLLMClient([])
    assert await l2_classify_batch([], client) == []
    assert client.calls == []


async def test_missing_index_raises():
    client = FakeLLMClient(['[{"index": 0, "is_failure": true, "reason": "x"}]'])
    with pytest.raises(ParseError):
        await l2_classify_batch(["a", "b"], client)
```

- [ ] **Step 8: Run tests to verify they fail**

Run: `pytest tests/triage/test_l2.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.triage.l2'`

- [ ] **Step 9: Write `src/rca/triage/l2.py`**

```python
import json

from pydantic import BaseModel

from rca.llm.parsing import ParseError, parse_json_list
from rca.llm.protocol import LLMClient
from rca.triage.models import TriageDecision

SYSTEM_PROMPT = (
    "You classify application alert email subjects. For each numbered subject, decide "
    "whether it reports an application failure requiring investigation, or is purely "
    "informational. Reply with a JSON array only, no prose and no code fences. Each "
    'element must be {"index": <int>, "is_failure": <bool>, "reason": "<short reason>"}. '
    "Return exactly one element per input subject."
)


class _Item(BaseModel):
    index: int
    is_failure: bool
    reason: str


async def l2_classify_batch(subjects: list[str], client: LLMClient) -> list[TriageDecision]:
    """Classify a batch of ambiguous subjects in a single LLM call."""
    if not subjects:
        return []

    user = json.dumps([{"index": i, "subject": s} for i, s in enumerate(subjects)])
    raw = await client.complete(system=SYSTEM_PROMPT, user=user)
    items = parse_json_list(raw, _Item)

    by_index = {item.index: item for item in items}
    if set(by_index) != set(range(len(subjects))):
        raise ParseError(
            f"expected indices 0..{len(subjects) - 1}, got {sorted(by_index)}"
        )

    return [
        TriageDecision(is_failure=by_index[i].is_failure, stage="L2", reason=by_index[i].reason)
        for i in range(len(subjects))
    ]
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `pytest tests/triage tests/llm -v`
Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add src/rca/llm src/rca/triage/l2.py tests/llm tests/triage/test_l2.py
git commit -m "feat: LLM client protocol and batched L2 subject classifier"
```

---

### Task 6: Alert envelope extraction

**Files:**
- Create: `src/rca/ingest/envelope.py`
- Test: `tests/ingest/test_envelope.py`

**Interfaces:**
- Consumes: `LLMClient` from Task 5, `parse_json_list` from Task 5
- Produces: `AlertEnvelope` (Pydantic), `extract_deterministic(text: str) -> dict`, `async extract_envelope(text: str, client: LLMClient) -> AlertEnvelope`

- [ ] **Step 1: Write the failing tests**

```python
# tests/ingest/test_envelope.py
from datetime import datetime, timezone

from rca.ingest.envelope import extract_deterministic, extract_envelope
from tests.llm.fakes import FakeLLMClient

SAMPLE = (
    "2026-09-08T14:22:31Z [PROD] host=ordsvc-prod-04 "
    "java.lang.NullPointerException at com.broadridge.order.OrderService.process"
)


def test_deterministic_extracts_timestamp():
    got = extract_deterministic(SAMPLE)
    assert got["occurred_at"] == datetime(2026, 9, 8, 14, 22, 31, tzinfo=timezone.utc)


def test_deterministic_extracts_host_and_environment():
    got = extract_deterministic(SAMPLE)
    assert got["server"] == "ordsvc-prod-04"
    assert got["environment"] == "prod"


def test_deterministic_returns_empty_when_nothing_matches():
    assert extract_deterministic("something went wrong") == {}


async def test_llm_fills_only_missing_fields():
    client = FakeLLMClient(['[{"application": "OrderService", "alert_type": "splunk",'
                           ' "summary": "Null pointer during order processing"}]'])
    env = await extract_envelope(SAMPLE, client)
    assert env.application == "OrderService"
    assert env.server == "ordsvc-prod-04"          # from deterministic pass
    assert env.occurred_at is not None             # never asked of the LLM


async def test_llm_failure_still_yields_envelope():
    client = FakeLLMClient(["not json at all"])
    env = await extract_envelope(SAMPLE, client)
    assert env.server == "ordsvc-prod-04"
    assert env.application is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/ingest/test_envelope.py -v`
Expected: FAIL with `ImportError: cannot import name 'extract_deterministic'`

- [ ] **Step 3: Write `src/rca/ingest/envelope.py`**

```python
import re
from datetime import datetime

import structlog
from dateutil import parser as date_parser
from pydantic import BaseModel

from rca.llm.parsing import ParseError, parse_json_list
from rca.llm.protocol import LLMClient

log = structlog.get_logger(__name__)

_TIMESTAMP = re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
_HOST = re.compile(r"\bhost(?:name)?[=:]\s*([A-Za-z0-9._-]+)", re.IGNORECASE)
_ENV = re.compile(r"\b(prod|uat|sit|qa|dev)\b", re.IGNORECASE)

SYSTEM_PROMPT = (
    "Extract alert metadata from the text. Reply with a JSON array containing exactly "
    'one object: {"application": <string|null>, "alert_type": <string|null>, '
    '"summary": "<one sentence>"}. No prose, no code fences.'
)


class AlertEnvelope(BaseModel):
    occurred_at: datetime | None = None
    application: str | None = None
    server: str | None = None
    environment: str | None = None
    alert_type: str | None = None
    summary: str = ""


class _LLMFields(BaseModel):
    application: str | None = None
    alert_type: str | None = None
    summary: str = ""


def extract_deterministic(text: str) -> dict:
    """Pull fields that can be found by pattern. Free and never hallucinated."""
    found: dict = {}

    if match := _TIMESTAMP.search(text):
        try:
            found["occurred_at"] = date_parser.isoparse(match.group(0))
        except ValueError:
            pass

    if match := _HOST.search(text):
        found["server"] = match.group(1)

    if match := _ENV.search(text):
        found["environment"] = match.group(1).lower()

    return found


async def extract_envelope(text: str, client: LLMClient) -> AlertEnvelope:
    """Deterministic extraction first; the LLM only fills what is left."""
    deterministic = extract_deterministic(text)

    try:
        raw = await client.complete(system=SYSTEM_PROMPT, user=text)
        fields = parse_json_list(raw, _LLMFields)[0]
    except (ParseError, IndexError) as exc:
        log.warning("envelope_llm_extraction_failed", error=str(exc))
        fields = _LLMFields()

    return AlertEnvelope(
        application=fields.application,
        alert_type=fields.alert_type,
        summary=fields.summary,
        **deterministic,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/ingest/test_envelope.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/rca/ingest/envelope.py tests/ingest/test_envelope.py
git commit -m "feat: alert envelope extraction, deterministic fields first"
```

---

### Task 7: Pipeline orchestration

Wires Tasks 2 to 6 into one function, with the redaction rule enforced by a test.

**Files:**
- Create: `src/rca/ingest/pipeline.py`
- Create: `src/rca/ingest/models.py`
- Test: `tests/ingest/test_pipeline.py`

**Interfaces:**
- Consumes: `normalize_text`, `redact`, `l1_classify`, `l2_classify_batch`, `extract_envelope`, `Incident`, `IncidentStatus`
- Produces: `RawAlert` (Pydantic: `source`, `external_id`, `subject`, `body`, `received_at`), `async process_alert(raw: RawAlert, client: LLMClient, session: AsyncSession) -> Incident`

- [ ] **Step 1: Write the failing tests**

```python
# tests/ingest/test_pipeline.py
from datetime import datetime, timezone

from rca.db.models import IncidentStatus
from rca.ingest.models import RawAlert
from rca.ingest.pipeline import process_alert
from tests.llm.fakes import FakeLLMClient


def _raw(subject: str, body: str = "generic body") -> RawAlert:
    return RawAlert(
        source="webhook",
        external_id="ext-1",
        subject=subject,
        body=body,
        received_at=datetime.now(timezone.utc),
    )


async def test_l1_informational_short_circuits(db_session):
    client = FakeLLMClient([])
    inc = await process_alert(_raw("Nightly backup completed successfully"), client, db_session)
    assert inc.status == IncidentStatus.DISCARDED_L1
    assert client.calls == []


async def test_failure_reaches_triaged(db_session):
    client = FakeLLMClient(['[{"application": "OrderService", "alert_type": "splunk",'
                           ' "summary": "NPE during processing"}]'])
    inc = await process_alert(_raw("CRITICAL: NullPointerException in OrderService"), client, db_session)
    assert inc.status == IncidentStatus.TRIAGED
    assert inc.envelope["application"] == "OrderService"


async def test_body_is_redacted_before_persist(db_session):
    client = FakeLLMClient(['[{"application": null, "alert_type": null, "summary": "s"}]'])
    body = "exception for account 1234567890123456 from jane@broadridge.com"
    inc = await process_alert(_raw("CRITICAL: exception", body), client, db_session)
    assert "1234567890123456" not in inc.body_redacted
    assert "jane@broadridge.com" not in inc.body_redacted


async def test_llm_never_sees_unredacted_text(db_session):
    client = FakeLLMClient(['[{"application": null, "alert_type": null, "summary": "s"}]'])
    await process_alert(_raw("CRITICAL: exception", "acct 1234567890123456"), client, db_session)
    for _, user in client.calls:
        assert "1234567890123456" not in user
```

Add a `db_session` fixture in `tests/conftest.py` using `testcontainers[postgres]` that creates the schema, yields an `AsyncSession`, and rolls back after each test.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/ingest/test_pipeline.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.ingest.pipeline'`

- [ ] **Step 3: Write `src/rca/ingest/models.py`**

```python
from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class RawAlert(BaseModel):
    source: Literal["webhook", "mailbox"]
    external_id: str
    subject: str
    body: str
    received_at: datetime
```

- [ ] **Step 4: Write `src/rca/ingest/pipeline.py`**

```python
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from rca.db.models import Incident, IncidentStatus
from rca.ingest.envelope import extract_envelope
from rca.ingest.models import RawAlert
from rca.ingest.normalize import normalize_text
from rca.llm.protocol import LLMClient
from rca.redact.engine import redact
from rca.triage.l1 import l1_classify
from rca.triage.l2 import l2_classify_batch

log = structlog.get_logger(__name__)


async def process_alert(raw: RawAlert, client: LLMClient, session: AsyncSession) -> Incident:
    """Normalise, redact, triage, and extract. Returns the persisted Incident."""
    incident = Incident(
        source=raw.source,
        external_id=raw.external_id,
        subject=raw.subject,
        received_at=raw.received_at,
    )

    decision = l1_classify(raw.subject)
    if decision is None:
        decision = (await l2_classify_batch([raw.subject], client))[0]

    incident.triage_reason = decision.reason

    if not decision.is_failure:
        incident.status = (
            IncidentStatus.DISCARDED_L1 if decision.stage == "L1" else IncidentStatus.DISCARDED_L2
        )
        session.add(incident)
        await session.flush()
        log.info("alert_discarded", external_id=raw.external_id, stage=decision.stage)
        return incident

    # Redaction must happen before persistence and before any further LLM call.
    redacted = redact(normalize_text(raw.body))
    incident.body_redacted = redacted.text

    envelope = await extract_envelope(redacted.text, client)
    incident.envelope = envelope.model_dump(mode="json")
    incident.status = IncidentStatus.TRIAGED

    session.add(incident)
    await session.flush()
    log.info("alert_triaged", external_id=raw.external_id)
    return incident
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/ingest -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/rca/ingest tests/ingest tests/conftest.py
git commit -m "feat: ingestion pipeline with redaction enforced before persistence"
```

---

### Task 8: Webhook and mailbox adapters

**Files:**
- Create: `src/rca/api/__init__.py`
- Create: `src/rca/api/app.py`
- Create: `src/rca/api/webhook.py`
- Create: `src/rca/ingest/mailbox.py`
- Create: `src/rca/worker.py`
- Test: `tests/api/test_webhook.py`, `tests/ingest/test_mailbox.py`

**Interfaces:**
- Consumes: `RawAlert`, `process_alert`
- Produces: FastAPI `app`; `POST /alerts/splunk`; `async poll_mailbox(graph_client, since) -> list[RawAlert]`; ARQ `WorkerSettings`

- [ ] **Step 1: Write the failing webhook tests**

```python
# tests/api/test_webhook.py
from httpx import ASGITransport, AsyncClient

from rca.api.app import app


async def test_webhook_accepts_and_returns_202():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/alerts/splunk", json={
            "sid": "splunk-abc",
            "search_name": "OrderService errors",
            "result": {"_raw": "NullPointerException at OrderService"},
        })
    assert r.status_code == 202


async def test_webhook_rejects_payload_without_sid():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/alerts/splunk", json={"search_name": "x"})
    assert r.status_code == 422


async def test_duplicate_sid_is_accepted_idempotently():
    payload = {"sid": "dup-1", "search_name": "x", "result": {"_raw": "boom"}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        first = await c.post("/alerts/splunk", json=payload)
        second = await c.post("/alerts/splunk", json=payload)
    assert first.status_code == 202
    assert second.status_code == 202
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/api/test_webhook.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.api.app'`

- [ ] **Step 3: Write `src/rca/api/webhook.py`**

```python
from datetime import datetime, timezone

from arq.connections import ArqRedis
from fastapi import APIRouter, Request, status
from pydantic import BaseModel

from rca.ingest.models import RawAlert

router = APIRouter()


class SplunkWebhookPayload(BaseModel):
    sid: str
    search_name: str
    result: dict


@router.post("/alerts/splunk", status_code=status.HTTP_202_ACCEPTED)
async def receive_splunk_alert(payload: SplunkWebhookPayload, request: Request) -> dict:
    raw = RawAlert(
        source="webhook",
        external_id=payload.sid,
        subject=payload.search_name,
        body=payload.result.get("_raw", ""),
        received_at=datetime.now(timezone.utc),
    )
    redis: ArqRedis = request.app.state.redis
    await redis.enqueue_job("process_alert_job", raw.model_dump(mode="json"), _job_id=payload.sid)
    return {"accepted": True}
```

Passing `_job_id=payload.sid` makes redelivery idempotent: ARQ refuses a duplicate job id, so a Splunk retry does not create a second investigation.

- [ ] **Step 4: Write `src/rca/api/app.py`**

```python
from contextlib import asynccontextmanager

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI

from rca.api.webhook import router
from rca.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await create_pool(RedisSettings.from_dsn(get_settings().redis_url))
    yield
    await app.state.redis.close()


app = FastAPI(title="RCA Agent", lifespan=lifespan)
app.include_router(router)
```

- [ ] **Step 5: Run webhook tests to verify they pass**

Run: `pytest tests/api -v`
Expected: PASS

- [ ] **Step 6: Write the failing mailbox test**

```python
# tests/ingest/test_mailbox.py
from datetime import datetime, timezone

from rca.ingest.mailbox import to_raw_alerts


class _Msg:
    def __init__(self, mid, subject, body):
        self.id = mid
        self.subject = subject
        self.body = type("B", (), {"content": body})()
        self.received_date_time = datetime.now(timezone.utc)


def test_maps_graph_messages_to_raw_alerts():
    alerts = to_raw_alerts([_Msg("m1", "CRITICAL: NPE", "<p>stack</p>")])
    assert alerts[0].source == "mailbox"
    assert alerts[0].external_id == "m1"
    assert alerts[0].subject == "CRITICAL: NPE"


def test_skips_messages_without_subject():
    assert to_raw_alerts([_Msg("m2", None, "body")]) == []
```

- [ ] **Step 7: Run test to verify it fails**

Run: `pytest tests/ingest/test_mailbox.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rca.ingest.mailbox'`

- [ ] **Step 8: Write `src/rca/ingest/mailbox.py`**

```python
from typing import Any

from rca.ingest.models import RawAlert


def to_raw_alerts(messages: list[Any]) -> list[RawAlert]:
    """Map Microsoft Graph message objects onto RawAlert, skipping unusable ones."""
    alerts: list[RawAlert] = []
    for msg in messages:
        if not getattr(msg, "subject", None):
            continue
        body = getattr(getattr(msg, "body", None), "content", "") or ""
        alerts.append(
            RawAlert(
                source="mailbox",
                external_id=msg.id,
                subject=msg.subject,
                body=body,
                received_at=msg.received_date_time,
            )
        )
    return alerts
```

- [ ] **Step 9: Write `src/rca/worker.py`**

```python
from arq.connections import RedisSettings

from rca.config import get_settings
from rca.db.session import get_session
from rca.ingest.models import RawAlert
from rca.ingest.pipeline import process_alert
from rca.llm.protocol import LLMClient


def _build_llm_client() -> LLMClient:
    # Replaced by the concrete BroadGPT client in Phase 2.
    raise NotImplementedError("BroadGPT client lands in Phase 2")


async def process_alert_job(ctx: dict, raw_payload: dict) -> None:
    raw = RawAlert.model_validate(raw_payload)
    async with get_session() as session:
        await process_alert(raw, ctx["llm"], session)
        await session.commit()


async def startup(ctx: dict) -> None:
    ctx["llm"] = _build_llm_client()


class WorkerSettings:
    functions = [process_alert_job]
    on_startup = startup
    redis_settings = RedisSettings.from_dsn(get_settings().redis_url)
```

- [ ] **Step 10: Run the full suite**

Run: `pytest -v`
Expected: PASS

- [ ] **Step 11: Commit**

```bash
git add src/rca/api src/rca/ingest/mailbox.py src/rca/worker.py tests/api tests/ingest/test_mailbox.py
git commit -m "feat: webhook and mailbox adapters with idempotent enqueue"
```

---

## Phase 1 done when

- A Splunk webhook POST results in a persisted `Incident` with a populated `envelope`.
- Informational mail is discarded at L1 without any LLM call.
- No test can find an unredacted identifier in `body_redacted` or in any prompt.
- The whole suite runs offline with no Splunk, GitLab, or BroadGPT credentials.

## Subsequent plans

Each needs its own plan document, written when its phase starts:

- **Phase 2** — BroadGPT client with both tool-calling modes, Splunk and GitLab collectors, evidence store, fingerprint cache. Replaces `_build_llm_client`.
- **Phase 3** — LangGraph supervisor, specialists, synthesis, critic, budgets, confidence derivation, checkpointing.
- **Phase 4** — React UI with evidence drill-down, SSE progress, follow-up chat, pgvector recall, feedback capture.
