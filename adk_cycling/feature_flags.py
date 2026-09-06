"""Env-var-driven feature flags. Deploy-time only, deliberately no Settings UI
surface — flipping one back is a redeploy, not a user-facing toggle."""
from __future__ import annotations

import os


def _flag(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes")


# When on: the user's real Garmin Coach plan (synced by the pipeline into GCS —
# see garmin_plan_store.py/garmin_plan_sync.py) drives the /plan page and chat
# plan tools instead of this app's own AI-generated plan. The custom-plan code
# (plan_generator.py, agent.create_training_plan, /api/plan/create) stays in
# place, just unregistered/unrouted while this is on — flip it back to restore
# that flow with zero code changes.
USE_GARMIN_COACHING_PLAN = _flag("USE_GARMIN_COACHING_PLAN", "true")
