"""Utility functions for bioc-converter."""

import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def safe_int(value: Any) -> Optional[int]:
    """Convert value to int if possible, return None otherwise.

    Args:
        value: Value to convert

    Returns:
        Integer value or None if conversion fails
    """
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def format_date_string(date_info: Dict[str, Any]) -> Optional[str]:
    """Return an ISO-ish date string (YYYY, YYYY-MM, or YYYY-MM-DD).

    Args:
        date_info: Dictionary with year, month, day keys

    Returns:
        Formatted date string or None
    """
    if not date_info:
        return None

    year = safe_int(date_info.get("year"))
    if not year:
        return None

    month = safe_int(date_info.get("month"))
    day = safe_int(date_info.get("day"))

    if month and day:
        return f"{year:04d}-{month:02d}-{day:02d}"
    if month:
        return f"{year:04d}-{month:02d}"
    return f"{year:04d}"


def sanitize_section_name(section_name: str) -> str:
    """Sanitize section name for use in filenames.

    Args:
        section_name: Raw section name

    Returns:
        Sanitized section name with only alphanumeric and underscores
    """
    cleaned = re.sub(r"[^a-z0-9]+", "_", section_name.lower()).strip("_")
    return cleaned or "section"


def make_random_id() -> str:
    """Generate a random UUID hex string for document IDs.

    Returns:
        32-character hex string
    """
    return uuid.uuid4().hex


def remove_overlapping_spans(
    spans: List[Tuple[int, int, str]]
) -> List[Tuple[int, int, str]]:
    """Remove overlapping spans, keeping the first one when overlaps occur.

    Args:
        spans: List of (start, end, label) tuples

    Returns:
        List of non-overlapping spans
    """
    if not spans:
        return []

    # Sort spans by their start position
    sorted_spans = sorted(spans, key=lambda x: x[0])
    non_overlapping_spans = []

    for span in sorted_spans:
        if not non_overlapping_spans:
            non_overlapping_spans.append(span)
        else:
            prev_span = non_overlapping_spans[-1]
            # Check for overlap and add the span if it doesn't overlap
            if span[0] >= prev_span[1]:
                non_overlapping_spans.append(span)

    return non_overlapping_spans


def format_annotations(
    text: str, annotations: List[Dict[str, Any]]
) -> Tuple[str, List[Tuple[int, int, str]]]:
    """Convert annotations to format suitable for processing.

    Removes overlapping spans.

    Args:
        text: The text content
        annotations: List of annotation dicts with 'span' (begin, end) and 'obj' (label)

    Returns:
        Tuple of (text, list of (start, end, label) tuples)
    """
    if not annotations:
        return (text, [])

    # Extract spans: (begin, end, label)
    spans = []
    for annot in annotations:
        span_info = annot.get("span", {})
        begin = span_info.get("begin", span_info.get("start", 0))
        end = span_info.get("end", span_info.get("begin", 0) + span_info.get("length", 0))
        label = annot.get("obj", annot.get("label", annot.get("type", "ENTITY")))
        spans.append((begin, end, label))

    # Remove overlapping spans
    non_overlapping_spans = remove_overlapping_spans(spans)

    return (text, non_overlapping_spans)


def load_json_config(config_path: str) -> Optional[Dict[str, Any]]:
    """Load a JSON configuration file.

    Args:
        config_path: Path to JSON file

    Returns:
        Parsed JSON as dictionary, or None if file doesn't exist

    Raises:
        ValueError: If JSON is invalid
    """
    config_file = Path(config_path)
    if not config_file.exists():
        return None

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")

