"""Academic text-quality metrics for ``/history compare``.

This module replaces "winner = highest logprob" with a multi-tier
framework that surfaces actual *correctness/quality* of the LLM-
generated descriptions. Three tiers, opt-in by cost:

* **Tier 0** — offline, deterministic, free.
  Type-token ratio (Templin 1957), schema grounding (Jaccard 1912
  token containment), chrF (Popović 2015), ROUGE-L (Lin 2004),
  Levenshtein edit distance (Levenshtein 1966).

* **Tier 1** — local sentence embeddings (free, opt-in).
  Pairwise cosine agreement matrix and semantic schema grounding
  via ``sentence-transformers`` (default ``all-MiniLM-L6-v2``).
  BERTScore (Zhang et al. 2020) is a separate Tier 1.5 toggle that
  loads the heavier ``bert-score`` package.

* **Tier 2** — LLM-as-judge, opt-in (consumes tokens on the
  active LLM provider).
  G-Eval style pairwise tournament (Liu et al. 2023, Kim et al.
  2024 Prometheus 2). Each asset gets ``C(N, 2)`` judge calls,
  output is structured JSON ``{winner, reasoning, confidence}``,
  per-run win-rate is the headline aggregate. Token usage is
  audited on the ``app_events`` trail so /usage aggregates can
  surface it; the analyze runs' own ``tokens_json`` snapshots
  stay untouched.

Reference selection follows a waterfall:

  1. **User override** — explicit ``ground_truth_run_id`` (CLI
     ``--ground-truth-run`` / Studio "Set as ground truth" radio).
  2. **Live DB COMMENT** — ``DatabaseConnector.get_column_comments``
     / ``get_table_comment``. SQL standard, the most authoritative
     ground-truth proxy when the team has already documented the
     column upstream.
  3. **Catalog applied** — most recent ``apply_events`` row for the
     same (schema, table, column).
  4. **None** — reference-based metrics (chrF / ROUGE / BERTScore)
     are skipped for that asset; only reference-free metrics run.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import combinations
from typing import Any

# ── Academic references (cited in UI / PDF / CHANGELOG / docs) ──────────────

ACADEMIC_REFERENCES: dict[str, dict[str, str]] = {
    "chrf": {
        "label": "chrF",
        "citation": (
            "Popović, M. (2015). chrF: character n-gram F-score for "
            "automatic MT evaluation. Proceedings of the Tenth Workshop "
            "on Statistical Machine Translation, 392–395."
        ),
        "url": "https://aclanthology.org/W15-3049/",
    },
    "rouge_l": {
        "label": "ROUGE-L",
        "citation": (
            "Lin, C.-Y. (2004). ROUGE: A Package for Automatic Evaluation "
            "of Summaries. Text Summarization Branches Out, ACL Workshop."
        ),
        "url": "https://aclanthology.org/W04-1013/",
    },
    "bertscore": {
        "label": "BERTScore",
        "citation": (
            "Zhang, T., Kishore, V., Wu, F., Weinberger, K. Q., & Artzi, "
            "Y. (2020). BERTScore: Evaluating Text Generation with BERT. "
            "ICLR 2020."
        ),
        "url": "https://arxiv.org/abs/1904.09675",
    },
    "g_eval": {
        "label": "G-Eval",
        "citation": (
            "Liu, Y., Iter, D., Xu, Y., Wang, S., Xu, R., & Zhu, C. "
            "(2023). G-Eval: NLG Evaluation using GPT-4 with Better "
            "Human Alignment. EMNLP 2023."
        ),
        "url": "https://arxiv.org/abs/2303.16634",
    },
    "prometheus": {
        "label": "Prometheus 2",
        "citation": (
            "Kim, S., Suk, J., Longpre, S., et al. (2024). Prometheus 2: "
            "An Open Source Language Model Specialized in Evaluating "
            "Other Language Models. EMNLP 2024."
        ),
        "url": "https://arxiv.org/abs/2405.01535",
    },
    "type_token_ratio": {
        "label": "Type-token ratio",
        "citation": (
            "Templin, M. C. (1957). Certain Language Skills in Children. "
            "University of Minnesota Press."
        ),
        "url": "",
    },
    "levenshtein": {
        "label": "Levenshtein distance",
        "citation": (
            "Levenshtein, V. I. (1966). Binary codes capable of "
            "correcting deletions, insertions, and reversals. Soviet "
            "Physics Doklady, 10(8), 707–710."
        ),
        "url": "",
    },
    "jaccard": {
        "label": "Jaccard similarity (schema grounding)",
        "citation": (
            "Jaccard, P. (1912). The Distribution of the Flora in the "
            "Alpine Zone. New Phytologist, 11(2), 37–50."
        ),
        "url": "",
    },
}


# ── Tier 0 helpers (pure Python, deterministic) ────────────────────────────


def _tokenize(text: str) -> list[str]:
    """Lower-case word-boundary tokenization. Splits camelCase and snake_case."""
    if not text:
        return []
    # Insert a space at camelCase boundaries before lower-casing so the two
    # halves of ``customerId`` survive as ``customer`` and ``id``.
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    return [t for t in re.split(r"[^a-zA-Z0-9]+", spaced.lower()) if t]


def _char_count(text: str) -> int:
    return len(text or "")


def _word_count(text: str) -> int:
    return len(_tokenize(text))


def type_token_ratio(text: str) -> float:
    """Vocabulary diversity in [0, 1] (Templin 1957).

    1.0 means every word is unique (rare, high diversity); low values
    flag repetition / boilerplate.
    """
    tokens = _tokenize(text)
    if not tokens:
        return 0.0
    return len(set(tokens)) / float(len(tokens))


# Common SQL dtype keywords used by the schema-grounding extractor.
# Kept lowercase so we can match against the lower-cased description.
_DTYPE_TOKENS: frozenset[str] = frozenset(
    {
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "varchar",
        "char",
        "text",
        "string",
        "date",
        "datetime",
        "timestamp",
        "time",
        "boolean",
        "bool",
        "json",
        "jsonb",
        "uuid",
        "blob",
        "binary",
        "bytea",
        "array",
        "map",
        "struct",
    }
)


def _schema_signal_tokens(
    schema: str | None,
    table: str | None,
    column: str | None,
    dtype: str | None,
) -> set[str]:
    """Tokens we expect a faithful description to surface — column /
    table / schema names plus the dtype family. The set is what the
    schema-grounding score tries to recover from the text."""
    bag: set[str] = set()
    bag.update(_tokenize(schema or ""))
    bag.update(_tokenize(table or ""))
    bag.update(_tokenize(column or ""))
    if dtype:
        # Pick out the canonical family token ("varchar(255)" → "varchar").
        for tok in _tokenize(dtype):
            if tok in _DTYPE_TOKENS:
                bag.add(tok)
        else:  # noqa: PLW0120 — falls through always; the loop body keeps additions
            pass
    bag.discard("")
    return bag


def schema_grounding_score(
    description: str,
    *,
    schema: str | None,
    table: str | None,
    column: str | None,
    dtype: str | None = None,
) -> float:
    """Faithfulness proxy in [0, 1] (Jaccard 1912 containment).

    A description that *names* the column it describes (and ideally its
    table / dtype) is anchored in the schema; a generic boilerplate is
    not. We return the fraction of expected schema tokens that appear
    in the lower-cased description.
    """
    expected = _schema_signal_tokens(schema, table, column, dtype)
    if not expected:
        return 0.0
    actual = set(_tokenize(description))
    if not actual:
        return 0.0
    overlap = expected.intersection(actual)
    return len(overlap) / float(len(expected))


def chrf_score(prediction: str, reference: str) -> float | None:
    """character n-gram F-score (Popović 2015) in [0, 1].

    Returns ``None`` when ``sacrebleu`` isn't installed (caller falls
    back to other reference-based metrics or skips the row).
    """
    if not prediction or not reference:
        return None
    try:
        from sacrebleu.metrics import CHRF
    except ImportError:
        return None
    metric = CHRF(word_order=0, char_order=6, beta=2)
    score = metric.sentence_score(prediction, [reference])
    # sacrebleu returns 0–100; normalize to 0–1 for consistency with
    # the rest of this module.
    return float(score.score) / 100.0


def rouge_l_score(prediction: str, reference: str) -> float | None:
    """ROUGE-L F1 (Lin 2004) in [0, 1].

    Returns ``None`` when ``rouge-score`` isn't installed.
    """
    if not prediction or not reference:
        return None
    try:
        from rouge_score import rouge_scorer
    except ImportError:
        return None
    scorer = rouge_scorer.RougeScorer(["rougeL"], use_stemmer=True)
    scores = scorer.score(reference, prediction)
    return float(scores["rougeL"].fmeasure)


# Module-level BERTScorer cache. Building a fresh ``BERTScorer`` is
# expensive (loads roberta-large, ~400 MB on first call) and prints
# a "Some weights of RobertaModel were not initialized..." warning
# on every load. Caching means we pay both costs ONCE per process —
# the AMX Studio terminal stays clean even when a Tier 1.5 quality
# pass scores 50 columns × 3 runs back-to-back.
_BERT_SCORER: Any = None


def bert_score_for_pair(prediction: str, reference: str) -> float | None:
    """BERTScore F1 (Zhang et al. 2020) in [0, 1].

    Tier 1.5: heavier than chrF / ROUGE-L because it loads a
    pretrained BERT model (~400MB on first call) and runs it on every
    description-reference pair, but it captures *semantic* similarity
    (paraphrases, synonym substitutions) where the n-gram metrics
    only see lexical overlap. Lazy-loaded via optional_deps.ensure
    so callers that don't opt into the ``bertscore`` extra don't pay
    the dependency at import time.

    The ``BERTScorer`` instance is cached at module level so the
    second-and-onward score calls in a tournament reuse the loaded
    model. Without the cache every pair re-loaded roberta-large
    and re-emitted the transformers' "Some weights..." warning,
    drowning the Studio terminal.

    Returns ``None`` when the package isn't installed (caller falls
    through gracefully) or when either input is empty.
    """
    global _BERT_SCORER
    if not prediction or not reference:
        return None
    try:
        from amx.utils.optional_deps import ensure

        ensure(
            [("bert_score", "bert-score")],
            feature="Compare BERTScore (Tier 1.5)",
        )
    except RuntimeError:
        return None
    try:
        from bert_score import BERTScorer
    except ImportError:
        return None

    # Defensive transformers version check. bert-score pulls tokenizers
    # 0.22+ on a fresh install, but if the user already has an older
    # transformers (4.51.x and below pin tokenizers<0.22) sitting in
    # the environment, the BERTScorer constructor either crashes with
    # a cryptic ``ImportError`` or silently scores with an incompatible
    # tokenizer. Surface a clean upgrade hint and skip cleanly so the
    # rest of the quality response still ships.
    try:
        import transformers as _transformers

        _ver = tuple(int(p) for p in _transformers.__version__.split(".")[:2])
        if _ver < (4, 56):
            from amx.utils.console import warn as _warn

            _warn(
                f"BERTScore (Tier 1.5) skipped — transformers "
                f"{_transformers.__version__} pre-dates the tokenizers "
                f"0.22 cutover. Run: pip install --upgrade "
                f"\"transformers>=4.56\""
            )
            return None
    except (ImportError, ValueError, AttributeError):
        # Couldn't parse the version string — let the BERTScorer
        # constructor decide. Real errors fall into the broader
        # ``except Exception`` below.
        pass

    # Silence transformers' "Some weights of RobertaModel were not
    # initialized..." warning AND any direct stderr writes the
    # tokenizer / accelerate libraries make during the first model
    # load. We re-use the same fd-level redirect the WeasyPrint path
    # uses (Pango stderr noise) so the Studio terminal stays quiet.
    if _BERT_SCORER is None:
        try:
            from transformers.utils import logging as _hf_logging

            _hf_logging.set_verbosity_error()
        except ImportError:
            pass
        try:
            with _silence_native_stderr():
                _BERT_SCORER = BERTScorer(
                    lang="en",
                    rescale_with_baseline=False,
                    nthreads=1,
                )
        except Exception:
            # First call downloads the model; in air-gapped / read-
            # only environments that download fails. Silently skip
            # BERTScore rather than poisoning the whole quality
            # response.
            _BERT_SCORER = None
            return None

    if _BERT_SCORER is None:
        return None
    try:
        with _silence_native_stderr():
            _, _, f1 = _BERT_SCORER.score([prediction], [reference])
    except Exception:
        return None
    return float(f1[0].item())


@contextlib.contextmanager
def _silence_native_stderr() -> Iterator[None]:
    """Redirect file-descriptor-level stderr to /dev/null for the
    duration of the block.

    BERTScore's first model load writes warnings via two channels:
    the transformers Python logger (silenced via
    ``hf_logging.set_verbosity_error``) AND raw stderr writes from
    the underlying C++ tokenizer / accelerate libraries that bypass
    Python logging entirely. Mirrors the WeasyPrint stderr-silencer
    in ``amx/cli_support/commands/compare.py`` — same dup2 dance.
    """
    try:
        stderr_fd = sys.stderr.fileno()
    except (AttributeError, ValueError, OSError):
        yield
        return
    saved_fd = os.dup(stderr_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull_fd, stderr_fd)
        yield
    finally:
        os.dup2(saved_fd, stderr_fd)
        os.close(devnull_fd)
        os.close(saved_fd)


def levenshtein_distance(prediction: str, reference: str) -> int | None:
    """Edit distance (Levenshtein 1966). 0 = identical, larger = farther.

    Uses ``difflib`` instead of forcing a new dependency — close enough
    for short descriptions and ships with the stdlib.
    """
    if not prediction or not reference:
        return None
    import difflib

    sm = difflib.SequenceMatcher(a=prediction, b=reference, autojunk=False)
    # Edit distance = total length - 2 * matched chars (close approximation
    # to true Levenshtein for the lengths we're dealing with here).
    matched = sum(triple.size for triple in sm.get_matching_blocks())
    return max(0, len(prediction) + len(reference) - 2 * matched)


# ── Reference resolution waterfall ──────────────────────────────────────────


@dataclass
class AssetReference:
    """Resolved ground-truth reference for one asset.

    ``source`` is one of ``"user_pinned"``, ``"db_comment"``,
    ``"catalog_applied"``, ``"none"``. ``text`` is empty when source
    is ``"none"`` — reference-based metrics MUST short-circuit when
    text is empty.
    """

    source: str
    text: str
    run_id: int | None


def resolve_reference_for_asset(
    *,
    schema: str,
    table: str,
    column: str | None,
    runs: list[dict[str, Any]],
    db_connector: Any,
    history_store: Any,
    ground_truth_run_id: int | None = None,
) -> AssetReference:
    """Walk the reference waterfall once per asset.

    Order:
      1. ``ground_truth_run_id`` — explicit user pin from CLI/Studio.
      2. Live DB ``COMMENT`` — most authoritative when populated.
      3. Most recent applied comment from ``apply_events``.
      4. Empty (caller will skip reference-based metrics).
    """
    # (1) User pin.
    if ground_truth_run_id is not None:
        for run in runs:
            try:
                if int(run.get("id") or 0) == int(ground_truth_run_id):
                    text = _description_from_run_for_asset(
                        run, schema, table, column, history_store
                    )
                    if text:
                        return AssetReference(
                            source="user_pinned", text=text, run_id=int(ground_truth_run_id)
                        )
            except (TypeError, ValueError):
                continue

    # (2) Live DB COMMENT.
    if db_connector is not None:
        try:
            if column:
                comments = db_connector.get_column_comments(schema, table) or {}
                live = comments.get(column)
            else:
                live = db_connector.get_table_comment(schema, table)
            if live:
                live_text = str(live).strip()
                if live_text:
                    return AssetReference(source="db_comment", text=live_text, run_id=None)
        except Exception:
            # The live DB might not be reachable, the schema/table may
            # have been dropped, or the user might not have read perms.
            # Silently fall through to the catalog history.
            pass

    # (3) Catalog applied.
    if history_store is not None and hasattr(history_store, "list_apply_events"):
        try:
            events = history_store.list_apply_events(
                run_id=None, profile_name=None, limit=200
            ) or []
            for ev in events:
                if (
                    ev.get("schema_name") == schema
                    and ev.get("table_name") == table
                    and (ev.get("column_name") or None) == (column or None)
                ):
                    txt = str(ev.get("new_comment") or "").strip()
                    if txt:
                        return AssetReference(source="catalog_applied", text=txt, run_id=None)
        except Exception:
            pass

    return AssetReference(source="none", text="", run_id=None)


def _description_from_run_for_asset(
    run: dict[str, Any],
    schema: str,
    table: str,
    column: str | None,
    history_store: Any,
) -> str:
    """Pull a run's description for one asset out of the history store.

    The pinned-baseline path needs the actual description text, not the
    summary the compare helper has already aggregated.
    """
    if history_store is None:
        return ""
    try:
        results = history_store.get_run_results(int(run["id"]))
    except Exception:
        return ""
    for r in results or []:
        if (
            r.get("schema_name") == schema
            and r.get("table_name") == table
            and (r.get("column_name") or None) == (column or None)
        ):
            return str(r.get("chosen_description") or "").strip() or _first_alternative(r)
    return ""


def _first_alternative(row: dict[str, Any]) -> str:
    """Fallback when ``chosen_description`` is empty: top alternative."""
    raw = row.get("alternatives_json")
    if isinstance(raw, list) and raw:
        first = raw[0]
        if isinstance(first, str):
            return first.strip()
        if isinstance(first, dict):
            return str(first.get("text") or first.get("description") or "").strip()
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                return _first_alternative({"alternatives_json": parsed})
        except json.JSONDecodeError:
            return raw.strip()
    return ""


# ── Tier 1 — local sentence embeddings ─────────────────────────────────────


def _load_sentence_embedder(
    model_name: str = "all-MiniLM-L6-v2",
) -> Any | None:
    """Lazy-load a sentence-transformers model. Returns ``None`` when
    the package isn't installed (caller falls through gracefully).
    """
    try:
        from amx.utils.optional_deps import ensure

        ensure(
            [("sentence_transformers", "sentence-transformers")],
            feature="Compare quality embeddings",
        )
    except RuntimeError:
        # ``ensure`` raises when pip install fails; treat as missing dep.
        return None
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return None
    return SentenceTransformer(model_name)


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def embedding_agreement_for_asset(
    descriptions_by_run: dict[int, str],
    embedder: Any,
) -> dict[int, float]:
    """For each run, mean cosine similarity to every other run on the
    same asset. High = the run agrees with the consensus; low = outlier.
    Empty descriptions are skipped from the matrix.
    """
    valid = {rid: t for rid, t in descriptions_by_run.items() if t}
    if len(valid) < 2:
        return {rid: 0.0 for rid in descriptions_by_run}
    rids = sorted(valid.keys())
    texts = [valid[r] for r in rids]
    vectors = embedder.encode(texts, show_progress_bar=False).tolist()
    by_run: dict[int, list[float]] = {rid: vec for rid, vec in zip(rids, vectors, strict=False)}
    agreement: dict[int, float] = {}
    for rid in rids:
        sims = [_cosine(by_run[rid], by_run[other]) for other in rids if other != rid]
        agreement[rid] = sum(sims) / float(len(sims)) if sims else 0.0
    # Runs whose description was empty get 0 agreement so the UI can
    # differentiate "missing" from "outlier".
    for rid in descriptions_by_run:
        agreement.setdefault(rid, 0.0)
    return agreement


def semantic_grounding_score(
    description: str,
    *,
    schema: str | None,
    table: str | None,
    column: str | None,
    dtype: str | None,
    embedder: Any,
) -> float:
    """Embedding-based version of schema grounding: how close is the
    description to a synthetic schema-anchor sentence?
    """
    if not description:
        return 0.0
    parts = [p for p in (schema, table, column) if p]
    anchor = ".".join(parts) if parts else ""
    if dtype:
        anchor = f"{anchor} ({dtype})"
    if not anchor:
        return 0.0
    vec_anchor, vec_desc = embedder.encode(
        [anchor, description], show_progress_bar=False
    ).tolist()
    return max(0.0, _cosine(vec_anchor, vec_desc))


# ── Tier 2 — LLM-as-judge (G-Eval pairwise tournament) ─────────────────────


@dataclass
class JudgeOutcome:
    """One pairwise judge result for one asset."""

    run_a: int
    run_b: int
    winner: str  # "A" | "B" | "tie"
    reasoning: str
    confidence: float
    prompt_tokens: int
    completion_tokens: int


_JUDGE_SYSTEM_PROMPT = (
    "You are an expert database documentation reviewer. Given a column's "
    "schema metadata and two candidate descriptions, choose which is more "
    "accurate, complete, and useful for a downstream developer. Reply with "
    "ONLY a single JSON object, no markdown fences, no commentary. Schema:\n"
    "  {\"winner\": \"A\"|\"B\"|\"tie\", "
    "\"reasoning\": \"<one short sentence>\", "
    "\"confidence\": <number 0-1>}"
)


def _build_judge_prompt(
    *,
    schema: str,
    table: str,
    column: str | None,
    dtype: str | None,
    reference: str,
    desc_a: str,
    desc_b: str,
) -> str:
    asset = ".".join(p for p in (schema, table, column) if p) or "(unknown)"
    lines = [
        f"Asset: {asset}",
    ]
    if dtype:
        lines.append(f"Type: {dtype}")
    if reference:
        lines.append(f"Reference (ground truth): {reference}")
    lines.append("")
    lines.append(f"Description A: {desc_a}")
    lines.append(f"Description B: {desc_b}")
    lines.append("")
    lines.append("Output JSON only.")
    return "\n".join(lines)


def _parse_judge_response(content: str) -> dict[str, Any] | None:
    """Best-effort JSON extraction from a judge LLM reply. Returns
    ``None`` when the reply isn't parseable so the caller can record
    a 'tie' default instead of crashing the whole tournament.
    """
    if not content:
        return None
    txt = content.strip()
    # Strip a leading code fence if the model defied the system prompt.
    if txt.startswith("```"):
        txt = re.sub(r"^```(?:json)?\s*", "", txt)
        txt = re.sub(r"\s*```\s*$", "", txt)
    try:
        parsed = json.loads(txt)
    except json.JSONDecodeError:
        # Try to grab the first JSON object substring.
        match = re.search(r"\{.*\}", txt, flags=re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def judge_pairwise(
    *,
    schema: str,
    table: str,
    column: str | None,
    dtype: str | None,
    reference: str,
    run_a: int,
    desc_a: str,
    run_b: int,
    desc_b: str,
    llm_provider: Any,
) -> JudgeOutcome:
    """One pairwise judgment via the active LLMProvider.

    Returns a tie with empty reasoning when the model fails to produce
    parseable JSON — keeps the tournament going instead of poisoning
    aggregates with exceptions.
    """
    user = _build_judge_prompt(
        schema=schema,
        table=table,
        column=column,
        dtype=dtype,
        reference=reference,
        desc_a=desc_a,
        desc_b=desc_b,
    )
    messages = [
        {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
    try:
        result = llm_provider.chat(messages, temperature=0.0, use_logprobs=False)
    except Exception as exc:
        return JudgeOutcome(
            run_a=run_a,
            run_b=run_b,
            winner="tie",
            reasoning=f"judge failed: {exc}",
            confidence=0.0,
            prompt_tokens=0,
            completion_tokens=0,
        )
    parsed = _parse_judge_response(result.content)
    usage = result.usage or {}
    p_tok = int(usage.get("prompt_tokens") or 0)
    c_tok = int(usage.get("completion_tokens") or 0)
    if not parsed:
        return JudgeOutcome(
            run_a=run_a,
            run_b=run_b,
            winner="tie",
            reasoning="judge response was not valid JSON",
            confidence=0.0,
            prompt_tokens=p_tok,
            completion_tokens=c_tok,
        )
    raw_winner = str(parsed.get("winner") or "tie").upper()
    if raw_winner not in {"A", "B", "TIE"}:
        raw_winner = "TIE"
    try:
        confidence = float(parsed.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return JudgeOutcome(
        run_a=run_a,
        run_b=run_b,
        winner="tie" if raw_winner == "TIE" else raw_winner,
        reasoning=str(parsed.get("reasoning") or "").strip(),
        confidence=confidence,
        prompt_tokens=p_tok,
        completion_tokens=c_tok,
    )


# ── Aggregator: compute_quality_metrics ─────────────────────────────────────


def _asset_key(schema: str, table: str, column: str | None) -> tuple[str, str, str]:
    return (schema or "", table or "", column or "")


def _description_for(row: dict[str, Any]) -> str:
    return str(row.get("description") or "").strip()


def _used_metric_keys(tier: int, has_any_reference: bool) -> list[str]:
    """Which ACADEMIC_REFERENCES keys to surface in citations for the
    user-visible methods footer. Reference-based metrics are dropped
    when no asset had a resolvable reference."""
    keys = ["type_token_ratio", "jaccard"]
    if has_any_reference:
        keys += ["chrf", "rouge_l", "levenshtein"]
    if tier >= 1:
        # Sentence-transformers grounding is "general embedding" — we
        # cite BERTScore as the closest peer-reviewed analogue when
        # Tier 1.5 BERTScore is also requested. For pure Tier 1 we
        # don't add a separate citation; the technique is generic
        # cosine similarity.
        pass
    if tier >= 2:
        keys += ["g_eval", "prometheus"]
    return keys


def _build_citations(metric_keys: Iterable[str]) -> list[dict[str, str]]:
    """Return the subset of ACADEMIC_REFERENCES to render in the UI."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for k in metric_keys:
        if k in seen or k not in ACADEMIC_REFERENCES:
            continue
        ref = ACADEMIC_REFERENCES[k]
        out.append({"key": k, "label": ref["label"], "citation": ref["citation"], "url": ref.get("url", "")})
        seen.add(k)
    return out


def compute_quality_metrics(
    payload: dict[str, Any],
    *,
    tier: int = 0,
    db_connector: Any = None,
    history_store: Any = None,
    ground_truth_run_id: int | None = None,
    llm_provider: Any = None,
) -> dict[str, Any]:
    """Aggregate quality metrics into a payload-shaped dict.

    Returns a dict with keys:
      * ``per_asset``: list of one row per (asset, run) with the
        per-cell metric values.
      * ``per_run``: list of run-level rollups (mean / win-rate per
        metric).
      * ``references``: list of asset-level reference resolution rows
        (source, text, run_id) so the UI can show "Reference: DB
        comment" badges.
      * ``citations``: list of academic references to render in the
        methods footer.
      * ``cost``: dict with ``prompt_tokens`` / ``completion_tokens``
        / ``total_tokens`` for the Tier 2 judge (zero-filled when
        Tier <= 1).
      * ``tier``: the tier we actually ran (clamped to 0 if
        sentence-transformers / sacrebleu aren't installed).
    """
    runs = list(payload.get("runs") or [])
    per_column = list(payload.get("per_column") or [])
    if not runs or not per_column:
        return {
            "per_asset": [],
            "per_run": [],
            "references": [],
            "citations": [],
            "cost": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            "tier": tier,
        }

    # Group descriptions by asset so we can resolve a reference once
    # per asset and feed the tournament cleanly.
    by_asset: dict[tuple[str, str, str], dict[int, dict[str, Any]]] = {}
    for row in per_column:
        try:
            rid = int(row.get("run_id"))
        except (TypeError, ValueError):
            continue
        key = _asset_key(
            str(row.get("schema") or ""),
            str(row.get("table") or ""),
            str(row.get("column") or ""),
        )
        by_asset.setdefault(key, {})[rid] = row

    embedder = _load_sentence_embedder() if tier >= 1 else None
    references: list[dict[str, Any]] = []
    per_asset_rows: list[dict[str, Any]] = []
    judge_outcomes: list[JudgeOutcome] = []

    for (schema, table, column), runs_for_asset in by_asset.items():
        ref = resolve_reference_for_asset(
            schema=schema,
            table=table,
            column=column or None,
            runs=runs,
            db_connector=db_connector,
            history_store=history_store,
            ground_truth_run_id=ground_truth_run_id,
        )
        references.append(
            {
                "schema": schema,
                "table": table,
                "column": column,
                "source": ref.source,
                "text": ref.text,
                "run_id": ref.run_id,
            }
        )

        descriptions_by_run = {rid: _description_for(r) for rid, r in runs_for_asset.items()}

        # Tier 1 embedding agreement is per-asset, computed once.
        agreement: dict[int, float] = {}
        if embedder is not None:
            try:
                agreement = embedding_agreement_for_asset(descriptions_by_run, embedder)
            except Exception:
                agreement = {}

        # Per-run / per-asset cell metrics.
        for rid, row in runs_for_asset.items():
            desc = descriptions_by_run[rid]
            dtype = str(row.get("dtype") or row.get("column_type") or "")
            cell: dict[str, Any] = {
                "schema": schema,
                "table": table,
                "column": column,
                "run_id": rid,
                "type_token_ratio": type_token_ratio(desc),
                "schema_grounding": schema_grounding_score(
                    desc,
                    schema=schema,
                    table=table,
                    column=column or None,
                    dtype=dtype or None,
                ),
            }
            if ref.text:
                cell["chrf"] = chrf_score(desc, ref.text)
                cell["rouge_l"] = rouge_l_score(desc, ref.text)
                cell["levenshtein"] = levenshtein_distance(desc, ref.text)
                cell["reference_source"] = ref.source
                # Tier 1.5: BERTScore is only worth the BERT inference
                # when there's a real reference to compare against,
                # AND the user opted into Tier 1+ (paying for embedding
                # work). The helper short-circuits to ``None`` on
                # missing packages / first-run download failures so a
                # missing reference cell falls back cleanly.
                if tier >= 1:
                    cell["bertscore"] = bert_score_for_pair(desc, ref.text)
            else:
                cell["reference_source"] = "none"
            if embedder is not None:
                cell["embedding_agreement"] = agreement.get(rid, 0.0)
                try:
                    cell["semantic_grounding"] = semantic_grounding_score(
                        desc,
                        schema=schema,
                        table=table,
                        column=column or None,
                        dtype=dtype or None,
                        embedder=embedder,
                    )
                except Exception:
                    cell["semantic_grounding"] = None
            per_asset_rows.append(cell)

        # Tier 2 — pairwise judge tournament for this asset.
        if tier >= 2 and llm_provider is not None:
            run_ids = sorted(rid for rid, t in descriptions_by_run.items() if t)
            for a, b in combinations(run_ids, 2):
                outcome = judge_pairwise(
                    schema=schema,
                    table=table,
                    column=column or None,
                    dtype=str(runs_for_asset[a].get("dtype") or "") or None,
                    reference=ref.text,
                    run_a=a,
                    desc_a=descriptions_by_run[a],
                    run_b=b,
                    desc_b=descriptions_by_run[b],
                    llm_provider=llm_provider,
                )
                judge_outcomes.append(outcome)

    # ── Per-run aggregates ─────────────────────────────────────────────────
    run_ids = [int(r["id"]) for r in runs]
    per_run: dict[int, dict[str, Any]] = {
        rid: {
            "run_id": rid,
            "type_token_ratio": [],
            "schema_grounding": [],
            "chrf": [],
            "rouge_l": [],
            "bertscore": [],
            "levenshtein": [],
            "embedding_agreement": [],
            "semantic_grounding": [],
            "judge_wins": 0,
            "judge_pairings": 0,
        }
        for rid in run_ids
    }
    for cell in per_asset_rows:
        rid = cell["run_id"]
        target = per_run.get(rid)
        if target is None:
            continue
        for metric in (
            "type_token_ratio",
            "schema_grounding",
            "chrf",
            "rouge_l",
            "bertscore",
            "embedding_agreement",
            "semantic_grounding",
        ):
            v = cell.get(metric)
            if v is not None:
                target[metric].append(float(v))
        ld = cell.get("levenshtein")
        if ld is not None:
            target["levenshtein"].append(float(ld))

    for outcome in judge_outcomes:
        for rid in (outcome.run_a, outcome.run_b):
            target = per_run.get(rid)
            if target is None:
                continue
            target["judge_pairings"] += 1
        if outcome.winner == "A":
            per_run[outcome.run_a]["judge_wins"] += 1
        elif outcome.winner == "B":
            per_run[outcome.run_b]["judge_wins"] += 1
        # Tie: nobody gets a win, but both pairings counted above.

    def _mean(xs: list[float]) -> float | None:
        return sum(xs) / float(len(xs)) if xs else None

    per_run_rows: list[dict[str, Any]] = []
    for rid in run_ids:
        agg = per_run[rid]
        out = {
            "run_id": rid,
            "type_token_ratio": _mean(agg["type_token_ratio"]),
            "schema_grounding": _mean(agg["schema_grounding"]),
            "chrf": _mean(agg["chrf"]),
            "rouge_l": _mean(agg["rouge_l"]),
            "bertscore": _mean(agg["bertscore"]),
            "levenshtein": _mean(agg["levenshtein"]),
            "embedding_agreement": _mean(agg["embedding_agreement"]),
            "semantic_grounding": _mean(agg["semantic_grounding"]),
            "judge_win_rate": (
                agg["judge_wins"] / float(agg["judge_pairings"])
                if agg["judge_pairings"] > 0
                else None
            ),
            "judge_pairings": agg["judge_pairings"],
            "judge_wins": agg["judge_wins"],
        }
        per_run_rows.append(out)

    # Cost rollup for Tier 2 judge calls.
    cost_p = sum(o.prompt_tokens for o in judge_outcomes)
    cost_c = sum(o.completion_tokens for o in judge_outcomes)
    cost = {
        "prompt_tokens": cost_p,
        "completion_tokens": cost_c,
        "total_tokens": cost_p + cost_c,
    }

    # Audit the judge cost into ``app_events`` so it shows up in
    # ``/usage`` aggregates, the Studio Audit page, and the AMX
    # ``app_events`` SQLite trail. We deliberately do NOT mutate the
    # compared runs' own ``tokens_json`` — those rows are closed
    # historical records of the analyze runs that produced the
    # descriptions; tampering would falsify the original cost
    # snapshot. The judge cost is conceptually a /history compare
    # cost, not a per-run cost, and belongs alongside the existing
    # ``search_compare`` audit event.
    if (
        judge_outcomes
        and history_store is not None
        and hasattr(history_store, "log_event")
    ):
        try:
            history_store.log_event(
                event_type="quality_judge",
                status="success",
                command="search.compare",
                details={
                    "run_ids": run_ids,
                    "asset_count": len(by_asset),
                    "pairings": len(judge_outcomes),
                    "prompt_tokens": cost_p,
                    "completion_tokens": cost_c,
                    "total_tokens": cost_p + cost_c,
                },
            )
        except Exception:
            # Audit-log failure should never break the user-visible
            # quality response; just swallow and continue.
            pass

    has_any_reference = any(r["source"] != "none" for r in references)
    has_bertscore = any(
        r.get("bertscore") is not None for r in per_run_rows
    )
    metric_keys = _used_metric_keys(tier, has_any_reference)
    if has_bertscore and "bertscore" not in metric_keys:
        # BERTScore (Tier 1.5) only contributes a citation when the
        # ``bert-score`` extra is installed AND a reference was
        # resolved — otherwise the helper returned None and the
        # citation would mislead.
        metric_keys.append("bertscore")
    citations = _build_citations(metric_keys)

    return {
        "per_asset": per_asset_rows,
        "per_run": per_run_rows,
        "references": references,
        "judge_outcomes": [
            {
                "run_a": o.run_a,
                "run_b": o.run_b,
                "winner": o.winner,
                "reasoning": o.reasoning,
                "confidence": o.confidence,
            }
            for o in judge_outcomes
        ],
        "citations": citations,
        "cost": cost,
        "tier": tier,
    }


__all__ = [
    "ACADEMIC_REFERENCES",
    "AssetReference",
    "JudgeOutcome",
    "bert_score_for_pair",
    "chrf_score",
    "compute_quality_metrics",
    "embedding_agreement_for_asset",
    "judge_pairwise",
    "levenshtein_distance",
    "resolve_reference_for_asset",
    "rouge_l_score",
    "schema_grounding_score",
    "semantic_grounding_score",
    "type_token_ratio",
]
