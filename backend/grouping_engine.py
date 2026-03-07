"""GroupingEngine — groups producers/consumers by client.id regex or topic prefix."""

import re
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Group:
    id: str
    label: str
    members: list[str] = field(default_factory=list)
    total_throughput: float = 0.0


class GroupingEngine:
    """Groups producers and consumers by configurable regex."""

    def __init__(self, regex_pattern: str = r"-[a-z0-9]{5,}$"):
        self._pattern = re.compile(regex_pattern)

    @property
    def pattern(self) -> str:
        return self._pattern.pattern

    @pattern.setter
    def pattern(self, value: str):
        self._pattern = re.compile(value)

    def extract_group_key(self, client_id: str) -> str:
        """Strip instance suffix from client.id to get group key."""
        return self._pattern.sub("", client_id)

    def group_by_client_id(self, client_ids: list[str]) -> dict[str, Group]:
        """Group client IDs by their extracted group key."""
        groups: dict[str, Group] = {}
        for cid in client_ids:
            key = self.extract_group_key(cid)
            if not key:
                key = "unidentified"
            if key not in groups:
                groups[key] = Group(
                    id=f"group-{key}",
                    label=key,
                )
            groups[key].members.append(cid)
        return groups

    def group_by_topic_prefix(self, topic_names: list[str]) -> dict[str, list[str]]:
        """Group topics by their dot-separated prefix."""
        groups: dict[str, list[str]] = {}
        for name in topic_names:
            prefix = name.split(".")[0] if "." in name else name
            groups.setdefault(prefix, []).append(name)
        return groups

    def preview_grouping(self, client_ids: list[str], regex: str) -> dict[str, list[str]]:
        """Preview grouping with a test regex — for the settings panel."""
        try:
            pattern = re.compile(regex)
        except re.error:
            return {"error": client_ids}

        result: dict[str, list[str]] = {}
        for cid in client_ids:
            key = pattern.sub("", cid) or "unidentified"
            result.setdefault(key, []).append(cid)
        return result
