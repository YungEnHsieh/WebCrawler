from __future__ import annotations

import sys
import types
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

    def _score_head(self, name: str, urls: list[str]) -> np.ndarray:
        char_x = self.vectorizer.transform(urls)
        num_x = numeric_feature_matrix(urls, self.priors)
        num_x = self.artifact[f"{name}_scaler"].transform(num_x)
        x = sparse.hstack([char_x, num_x], format="csr")
        return self.artifact[f"{name}_head"].predict_proba(x)[:, 1]

    def score_many(self, urls: list[str]) -> list[float]:
        if not urls:
            return []

        # Production stores one operational score: the max confidence across
        # the child-url, parent, pathfinder, and refresh heads.
        scores = [self._score_head(name, urls) for name in self.heads]
        combined = np.maximum.reduce(scores)
        return [float(v) for v in np.clip(combined, 0.0, 1.0)]
