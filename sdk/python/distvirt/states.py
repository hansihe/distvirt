"""State matchers for wait_for() conditions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from distvirt.namespace import WorkloadModel, ServiceModel


@dataclass(frozen=True)
class WorkloadStateMatcher:
    """Matches a workload state by name."""

    state: str

    def matches(self, model: WorkloadModel) -> bool:
        return model.state == self.state

    def __repr__(self) -> str:
        return f"distvirt.{self.state}"


@dataclass(frozen=True)
class ServiceStateMatcher:
    """Matches a service state by name."""

    state: str

    def matches(self, model: ServiceModel) -> bool:
        return model.state == self.state

    def __repr__(self) -> str:
        return f"distvirt.{self.state}"


# Workload states
dormant = WorkloadStateMatcher("dormant")
launching = WorkloadStateMatcher("launching")
running = WorkloadStateMatcher("running")
completed = WorkloadStateMatcher("completed")
failed = WorkloadStateMatcher("failed")
suspended = WorkloadStateMatcher("suspended")

# Service states
idle = ServiceStateMatcher("idle")
active = ServiceStateMatcher("active")
