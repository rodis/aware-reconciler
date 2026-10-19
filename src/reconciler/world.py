"""The `World` implementation — where `core`'s decisions meet real side effects (ADR 0012).

`core.reconcile` decides; this dispatches. It resolves a stage's `action` string through the
registry, hands the action its own `config` block, and delegates recording to whatever sink
it was given (the gateway, or a dry run).

Kept separate from both so the composition stays visible in one small file: the pure decision
logic never sees a transport, and the adapters never see a stage.
"""

import logging

from reconciler.actions import ActionContext, Services, build_action
from reconciler.core import Cycle, Milestone
from reconciler.definition import ProcessDefinition

logger = logging.getLogger("reconciler.world")


class NotYetImplemented(RuntimeError):
    """Raised when a process reaches a stage whose machinery does not exist yet.

    Deliberately loud. A silently-skipped `await` would look exactly like "still waiting",
    which is the one failure this tier must never fake — a process that appears to be
    patiently waiting while nothing is watching is worse than one that stops.
    """


class RealWorld:
    """Dispatches `act`, `find` and `record` for one process."""

    def __init__(self, definition: ProcessDefinition, sink, services: Services | None = None,
                 finders: dict | None = None):
        self._definition = definition
        self._sink = sink
        self._services = services or Services()
        # Keyed by the signal's `source`, so a process can mix deterministic evidence
        # (a label someone applied) with interpreted evidence (an LLM reading prose) and
        # each stage says which it is.
        self._finders = finders or {}
        self._config_for = {stage.name: stage.config for stage in definition.stages}
        self._action_for = {stage.name: stage.action for stage in definition.stages}

    # `core` passes the action string, so map back to the stage to find its config block.
    def _stage_of(self, action: str) -> str | None:
        for stage, configured in self._action_for.items():
            if configured == action:
                return stage
        return None

    def act(self, action: str, cycle: Cycle, milestones: dict[str, Milestone]) -> dict:
        stage = self._stage_of(action)
        context = ActionContext(
            cycle=cycle,
            milestones=milestones,
            config=self._config_for.get(stage, {}),
            services=self._services,
        )
        try:
            run = build_action(action)
        except KeyError as e:
            # Same treatment as an unwired finder: a stage naming an action nobody wrote is
            # "the tier does not reach here yet", not a crash. Loud either way — what must
            # never happen is skipping it and looking like progress.
            raise NotYetImplemented(
                f"cycle {cycle.key} reached stage action {action!r}, which is not built yet"
            ) from e
        return run(context)

    def find(self, signal: dict, cycle: Cycle, since: int,
             milestones: dict[str, Milestone]) -> dict | None:
        finder = self._finders.get(signal.get("source"))
        if finder is None:
            raise NotYetImplemented(
                f"cycle {cycle.key} reached an await whose source is "
                f"{signal.get('source', '?')!r}, and no finder is wired for it"
            )
        return finder.find(signal, cycle, since, milestones)

    def record(self, cycle: Cycle, stage: str, payload: dict) -> Milestone:
        return self._sink.record(cycle, stage, payload)
