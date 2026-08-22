"""Load ML-1's actual trained artifact and score with it.

This closes the last pairing the specs call out: ML-1's model behind SE-3's
gateway. Until now `model_server.py` served a synthetic scoring function that
burned a calibrated amount of CPU -- which is the right stand-in for measuring
transport and contention, and is not a model.

What loading the real artifact changes, and why each matters here:

  THE FEATURE CONTRACT BECOMES REAL. ML-1's model expects nine features in a
  fixed order, two of which (`is_night`, `amount_per_velocity`) are DERIVED and
  must be computed exactly as training computed them. The gateway has a velocity
  counter and a feature cache; the model has an opinion about what those numbers
  mean. Getting the order wrong produces a model that runs and is nonsense, so
  the order comes from the artifact rather than from a constant here.

  THE THRESHOLD TRAVELS WITH THE MODEL. ML-1 stores the cost-optimal threshold
  and the calibrator alongside the weights. The gateway's own decision policy
  has its own thresholds for degradation tiers; those are different numbers for
  a different purpose and must not be confused. The model's threshold decides
  "is this fraud"; the gateway's tiers decide "what do we do when we cannot ask".

  LATENCY BECOMES HONEST. A LightGBM forest on nine features is fast -- much
  faster than the 8ms CPU stub -- so the measured budget will IMPROVE, and that
  improvement is not an optimisation. It means the earlier number was a property
  of the stub. Both are reported.

If the artifact is missing the loader says so and the caller falls back to the
synthetic scorer, rather than failing at request time.
"""
from __future__ import annotations

import pickle
from pathlib import Path

# ML-1 lives in a sibling repo in the portfolio layout.
ML1 = Path(__file__).resolve().parents[2] / "ml1-governed-fraud"
ARTIFACT = ML1 / "artifacts" / "model.pkl"


class ArtifactUnavailable(Exception):
    pass


class RealFraudModel:
    """ML-1's trained model, adapted to the gateway's request shape."""

    def __init__(self, artifact_path: Path | None = None):
        path = Path(artifact_path or ARTIFACT)
        if not path.exists():
            raise ArtifactUnavailable(
                "no ML-1 artifact at {}. Run `python train.py` in "
                "ml1-governed-fraud first.".format(path))
        import sys
        # The pickle references ML-1's own modules (the focal wrapper, the
        # calibrator), so its package has to be importable to unpickle at all.
        if str(ML1) not in sys.path:
            sys.path.insert(0, str(ML1))
        with path.open("rb") as fh:
            art = pickle.load(fh)

        self.model = art["model"]
        self.features = art["features"]          # ORDER comes from the artifact
        self.threshold = art["threshold"]
        self.calibrator = art.get("calibrator")
        self.model_version = art["model_version"]
        self.policy_version = art["policy_version"]

    def features_from(self, req_like: dict) -> list[float]:
        """Build the model's feature vector from gateway request fields.

        Derived features are computed HERE, the same way training computed them,
        rather than being accepted from a caller. A caller that disagrees with
        training about what `is_night` means produces a model that runs and is
        quietly wrong.
        """
        amount = float(req_like.get("amount_minor", 0))
        velocity = float(req_like.get("velocity_24h", 0))
        hour = int(req_like.get("hour", 12))
        d = {
            "amount_minor": amount,
            "velocity_24h": velocity,
            "cross_border": float(req_like.get("cross_border", 0)),
            "device_change": float(req_like.get("device_change", 0)),
            "mcc_risk": float(req_like.get("mcc_risk", 0.5)),
            "hour": float(hour),
            "card_tenure_days": float(req_like.get("card_tenure_days", 365)),
            "is_night": 1.0 if 1 <= hour <= 5 else 0.0,
            "amount_per_velocity": amount / (1.0 + velocity),
        }
        missing = [f for f in self.features if f not in d]
        if missing:
            raise ArtifactUnavailable(
                "gateway cannot supply model features: {}".format(missing))
        return [d[f] for f in self.features]

    def score(self, req_like: dict) -> float:
        import numpy as np

        x = np.array([self.features_from(req_like)], dtype=float)
        p = float(self.model.predict_proba(x)[:, 1][0])
        if self.calibrator is not None:
            # The calibrator is part of the model. Serving the raw score against
            # a calibrated threshold is a different decision, not a rounding one.
            p = float(self.calibrator.predict([p])[0])
        return p

    def info(self) -> dict:
        return {
            "model_version": self.model_version,
            "policy_version": self.policy_version,
            "threshold": self.threshold,
            "features": self.features,
            "calibrated": self.calibrator is not None,
        }


def try_load(artifact_path: Path | None = None):
    """Returns (model, None) or (None, reason)."""
    try:
        return RealFraudModel(artifact_path), None
    except Exception as exc:
        return None, str(exc)
