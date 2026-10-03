"""Pure helpers for non-sensitive Airflow failure events."""

from __future__ import annotations

from typing import Any


def failure_event(context: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-serializable failure payload from an Airflow context."""
    task_instance = context["task_instance"]
    return {
        "event": "task_failed",
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": context.get("run_id"),
        "try_number": task_instance.try_number,
        "exception": str(context.get("exception")),
    }
