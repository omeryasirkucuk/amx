"""Reference-based quality metrics for ``amx compare``.

Extracted from :mod:`amx.cli_support.quality` so the four reference
metrics (chrF, ROUGE-L, BERTScore, Levenshtein) and the native-stderr
silencer they share live in one focused module. Each metric is pure
(no AMX state) and lazy-imports its heavy dependency only when first
called.

``quality.py`` re-exports the public names so callers
(``compare.py`` via ``compute_quality_metrics``) keep working
unchanged.
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Iterator


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
                f'"transformers>=4.56"'
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
