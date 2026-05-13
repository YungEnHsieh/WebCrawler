from __future__ import annotations

import sys
import types
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from scipy import sparse

from .golden_discovery_features import C2PriorMaps, numeric_feature_matrix


GOLDEN_DISCOVERY_HEADS = ("url", "parent", "pathfinder", "refresh")


def _install_pickle_shims() -> None:
    """Let old artifacts unpickle without importing experiment scripts."""
    main_mod = sys.modules.get("__main__")
    if main_mod is not None:
        setattr(main_mod, "C2PriorMaps", C2PriorMaps)

    module_name = "scripts.train_c2_graph_aware_discriminator"
    shim = sys.modules.get(module_name) or types.ModuleType(module_name)
    setattr(shim, "C2PriorMaps", C2PriorMaps)
    sys.modules[module_name] = shim


class GoldenDiscoveryRuntimeScorer:
    """Loads a Golden Discovery Ranker artifact and scores URLs with its heads."""

    def __init__(self, artifact: dict[str, Any]):
        self.artifact = artifact
        self.vectorizer = artifact.get("vectorizer")
        self.priors = artifact.get("priors")
        self.heads = [
            name
            for name in GOLDEN_DISCOVERY_HEADS
            if artifact.get(f"{name}_head") is not None
            and artifact.get(f"{name}_scaler") is not None
        ]

        missing = [
            key for key, value in {"vectorizer": self.vectorizer, "priors": self.priors}.items()
            if value is None
        ]
        if missing:
            raise ValueError(
                f"Golden Discovery Ranker artifact missing required keys: {', '.join(missing)}"
            )
        if not self.heads:
            raise ValueError("Golden Discovery Ranker artifact has no usable scoring heads")

        # Long-lived thread pool reused across every score_many call.
        # ThreadPoolExecutor doesn't spawn threads at construction — they
        # are lazily created on the first submit() and then idle on the
        # work queue between calls, so an unused scorer pays no thread
        # cost. Reusing the pool removes ~300us–1.5ms of spawn+join
        # overhead per call, which dominates at small batch sizes (the
        # inline-ingestor scoring path that PR C will enable scores a
        # batch per ingest result and would otherwise re-create the pool
        # ~hundreds of times per second per worker).
        self._executor = ThreadPoolExecutor(
            max_workers=len(self.heads),
            thread_name_prefix="golden_score_head",
        )

    def close(self) -> None:
        """Shut the underlying thread pool down.

        Production scorer / ingestor processes are long-lived and rely on
        the interpreter's atexit shutdown to reclaim threads, so calling
        close() is optional in those paths. Tests and short-lived tooling
        should call it (or use the scorer as a context manager) to avoid
        leaving idle worker threads behind between test cases.
        """
        self._executor.shutdown(wait=True)

    def __enter__(self) -> "GoldenDiscoveryRuntimeScorer":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @classmethod
    def load(cls, path: str | Path) -> "GoldenDiscoveryRuntimeScorer":
        _install_pickle_shims()
        artifact = joblib.load(Path(path))
        if not isinstance(artifact, dict):
            raise ValueError("Golden Discovery Ranker artifact must be a dict")
        return cls(artifact)

    @property
    def metadata(self) -> dict[str, Any]:
        metadata = self.artifact.get("metadata") or {}
        return metadata if isinstance(metadata, dict) else {}

    def _score_head(
        self,
        name: str,
        char_x: sparse.spmatrix,
        num_x_raw: np.ndarray,
    ) -> np.ndarray:
        """Run one head given features that have already been built.

        `char_x` and `num_x_raw` are head-independent and computed once per
        `score_many` call; only the scaler.transform + hstack +
        predict_proba pipeline is per-head.
        """
        num_x = self.artifact[f"{name}_scaler"].transform(num_x_raw)
        x = sparse.hstack([char_x, num_x], format="csr")
        return self.artifact[f"{name}_head"].predict_proba(x)[:, 1]

    def score_many(self, urls: list[str]) -> list[float]:
        if not urls:
            return []

        # Compute the head-independent features ONCE per call. The
        # vectorizer.transform and numeric_feature_matrix calls dominate
        # per-call work outside the heads themselves; sharing the result
        # across heads removes ~(n_heads - 1)x of duplicate work.
        char_x = self.vectorizer.transform(urls)
        num_x_raw = numeric_feature_matrix(urls, self.priors)

        # Fan the per-head pipeline out to the long-lived thread pool
        # built in __init__. scikit-learn's predict_proba on the heads
        # used here delegates to BLAS / Cython kernels that release the
        # GIL, so threading actually parallelises the inference rather
        # than just interleaving Python. ThreadPoolExecutor is preferred
        # over ProcessPoolExecutor because the heads + sparse matrices
        # share memory between threads without pickling overhead.
        # Production stores one operational score: the max confidence
        # across the child-url, parent, pathfinder, and refresh heads.
        head_scores = list(
            self._executor.map(
                lambda name: self._score_head(name, char_x, num_x_raw),
                self.heads,
            )
        )

        combined = np.maximum.reduce(head_scores)
        return [float(v) for v in np.clip(combined, 0.0, 1.0)]
