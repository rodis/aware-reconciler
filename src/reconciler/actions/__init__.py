"""Stage actions — the `act` implementations (ADR 0012).

An action is resolved from a stage's `action` string through this registry, exactly as an
engine is resolved from a definition's `engine` string in `inference.engines`. And exactly as
there, **an action parses its own `config` block** — `core` never knows an action's config
schema, which is what lets a new action ship without touching the reconciler.

Actions that need the outside world take it from `ActionContext.services`, so the pure ones
(`lines.worked_days`, `compute.total`) stay importable and testable with no dependencies at
all. Every side effect in the tier lives under this package.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from reconciler.core import Cycle, Milestone

logger = logging.getLogger("reconciler.actions")

_REGISTRY: dict[str, "Action"] = {}


class ExtrasSource(Protocol):
    """Where manually-entered invoice lines come from.

    A port because the answer is deliberately unsettled (ADR 0012 open question 8): the n8n
    Data Table works today, a Neon table with a dashboard editor is the target, and swapping
    them must not be a design change.
    """

    def lines_for(self, cycle: Cycle) -> list[dict]: ...


class Mailer(Protocol):
    def send(self, *, subject: str, html: str, text: str) -> None: ...


class PdfRenderer(Protocol):
    def render(self, *, template_id: str, data: dict) -> dict: ...


@dataclass(frozen=True)
class Services:
    """Ports the impure actions need. Optional so a pure action needs none of them wired."""

    extras: ExtrasSource | None = None
    mailer: Mailer | None = None
    pdf: PdfRenderer | None = None


@dataclass(frozen=True)
class ActionContext:
    cycle: Cycle
    milestones: dict[str, Milestone]
    config: dict = field(default_factory=dict)      # the stage's own `config:` block
    services: Services = field(default_factory=Services)


Action = Callable[[ActionContext], dict]


def register_action(name: str) -> Callable[[Action], Action]:
    def wrap(fn: Action) -> Action:
        if name in _REGISTRY:
            raise ValueError(f"action {name!r} is already registered")
        _REGISTRY[name] = fn
        return fn
    return wrap


def build_action(name: str) -> Action:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"unknown action {name!r}; registered: {sorted(_REGISTRY)}"
        ) from None


def registered_actions() -> list[str]:
    return sorted(_REGISTRY)


# Importing the package registers the built-ins, mirroring inference/engines/__init__.py.
from reconciler.actions import (  # noqa: E402,F401  (side-effect imports)
    compute,
    lines,
    notify,
    render,
)
