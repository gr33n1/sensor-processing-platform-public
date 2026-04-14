from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class IssueSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(slots=True)
class DataQualityIssue:
    issue_type: str
    severity: IssueSeverity
    message: str
    column: str | None = None
    details: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ProcessingReport:
    row_count_before: int
    row_count_after: int
    missing_percent_by_column: dict[str, float]
    out_of_range_counts: dict[str, int]
    issues: list[DataQualityIssue] = field(default_factory=list)