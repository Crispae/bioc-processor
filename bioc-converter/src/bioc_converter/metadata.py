"""Metadata handling for Odinson documents.

This module provides utilities for building Odinson metadata fields
from BioC document metadata.
"""

from typing import Any, Dict, List, Optional

# Import clu-bridge odinson types
try:
    from clu.bridge import odinson
except ImportError as e:
    raise ImportError(
        "clu-bridge is required for Odinson conversion. "
        "Install it with: pip install git+https://github.com/clulab/clu-bridge.git "
        "or: cd clu-bridge && pip install -e ."
    ) from e

from .models import DocumentData, SentenceAnnotation
from .utils import format_date_string


def build_odinson_metadata_fields(
    doc_data: Dict[str, Any],
    section_name: Optional[str] = None,
) -> List[odinson.AnyField]:
    """Create Odinson metadata fields from stored BioC metadata.

    Args:
        doc_data: Document data dictionary with metadata_source
        section_name: Optional section name to include in metadata

    Returns:
        List of Odinson metadata field objects
    """
    metadata_fields: List[odinson.AnyField] = []
    metadata_source = doc_data.get("metadata_source") or {}
    if not metadata_source:
        return metadata_fields

    # Add document ID
    doc_id = doc_data.get("doc_id")
    if doc_id:
        metadata_fields.append(
            odinson.TokensField(name="doc_id", tokens=[str(doc_id)])
        )

    # Add publication date
    date_info = metadata_source.get("date") or {}
    date_str = format_date_string(date_info)
    if not date_str:
        # Fall back to BioC's date attribute if present
        date_str = metadata_source.get("bioc_date")
    if date_str:
        metadata_fields.append(
            odinson.DateField(name="pub_date", date=str(date_str))
        )

    # Add section information
    if section_name:
        metadata_fields.append(
            odinson.TokensField(name="section", tokens=[section_name])
        )
    else:
        metadata_fields.append(
            odinson.TokensField(name="section", tokens=["ALL"])
        )

    return metadata_fields


def build_metadata_from_document_data(
    doc_data: DocumentData,
    section_name: Optional[str] = None,
) -> List[odinson.AnyField]:
    """Create Odinson metadata fields from a DocumentData object.

    Args:
        doc_data: DocumentData object
        section_name: Optional section name to include in metadata

    Returns:
        List of Odinson metadata field objects
    """
    return build_odinson_metadata_fields(doc_data.to_dict(), section_name)


def group_sentence_annotations_by_section(
    sentence_annotations: List[SentenceAnnotation],
    sections_info: Optional[List[Dict[str, Any]]],
) -> Dict[str, List[SentenceAnnotation]]:
    """Group sentences by their section using section boundaries.

    Args:
        sentence_annotations: List of SentenceAnnotation objects
        sections_info: List of section info dicts with start, end, section_type

    Returns:
        Dictionary mapping section names to lists of SentenceAnnotation
    """
    sections_info = sections_info or []
    sorted_sections = sorted(
        sections_info,
        key=lambda section: section.get("start", 0),
    )

    def find_section_for_offset(offset: int) -> str:
        for section in sorted_sections:
            start = section.get("start")
            end = section.get("end")
            if start is None or end is None:
                continue
            if start <= offset < end:
                return section.get("section_type") or "UNLABELED"
        return "UNLABELED"

    grouped: Dict[str, List[SentenceAnnotation]] = {}
    for sent_ann in sentence_annotations:
        abs_start = sent_ann.abs_start or 0
        abs_end = sent_ann.abs_end or abs_start

        # Strategy: Check START first (most accurate for section assignment)
        section_name = find_section_for_offset(abs_start)

        # Fallback 1: Check midpoint if start doesn't match
        if section_name == "UNLABELED":
            midpoint = (abs_start + abs_end) // 2
            section_name = find_section_for_offset(midpoint)

        # Fallback 2: Check end (exclusive)
        if section_name == "UNLABELED" and abs_end > abs_start:
            section_name = find_section_for_offset(abs_end - 1)

        grouped.setdefault(section_name, []).append(sent_ann)

    return grouped


def create_section_metadata(
    doc_data: Dict[str, Any],
    section_name: str,
) -> List[odinson.AnyField]:
    """Create metadata fields for a specific section.

    Args:
        doc_data: Document data dictionary
        section_name: Name of the section

    Returns:
        List of Odinson metadata fields including section information
    """
    return build_odinson_metadata_fields(doc_data, section_name=section_name)

