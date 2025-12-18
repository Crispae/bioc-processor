"""BioC XML document loading and parsing.

This module handles loading BioC XML files and extracting document data
for conversion to Odinson format.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Import bioc - try different methods for compatibility
try:
    import bioc

    if hasattr(bioc, "load"):
        bioc_load = bioc.load
    else:
        from bioc import biocxml

        bioc_load = biocxml.load
except ImportError:
    from bioc import biocxml

    bioc_load = biocxml.load

from .models import DocumentData, SectionInfo


def load_section_config(config_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load section filtering configuration from JSON file.

    Args:
        config_path: Path to JSON configuration file. If None, returns None.

    Returns:
        Dictionary with 'allowed_sections' (list) and 'case_sensitive' (bool),
        or None if config_path is None or file doesn't exist.

    Raises:
        ValueError: If config file exists but has invalid structure.
    """
    if config_path is None:
        return None

    config_file = Path(config_path)
    if not config_file.exists():
        print(
            f"Warning: Section config file not found: {config_path}. "
            "Processing all sections."
        )
        return None

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in section config file: {e}")

    # Validate structure
    if not isinstance(config, dict):
        raise ValueError("Section config must be a dictionary")

    allowed_sections = config.get("allowed_sections", [])
    if not isinstance(allowed_sections, list):
        raise ValueError("'allowed_sections' must be a list")

    case_sensitive = config.get("case_sensitive", False)
    if not isinstance(case_sensitive, bool):
        raise ValueError("'case_sensitive' must be a boolean")

    # Normalize allowed sections if case-insensitive
    if not case_sensitive:
        allowed_sections = [s.upper() for s in allowed_sections if isinstance(s, str)]

    return {
        "allowed_sections": allowed_sections,
        "case_sensitive": case_sensitive,
    }


def is_section_allowed(
    section_type: Optional[str], section_config: Optional[Dict[str, Any]]
) -> bool:
    """Check if a section type is in the allowed sections list.

    Args:
        section_type: BioC section type (e.g., "ABSTRACT", "INTRO")
        section_config: Section configuration dict from load_section_config(), or None

    Returns:
        True if section_config is None (no filtering), or if section_type matches
        an allowed section. False otherwise.
    """
    if section_config is None:
        return True

    if not section_type:
        return False

    allowed_sections = section_config.get("allowed_sections", [])
    case_sensitive = section_config.get("case_sensitive", False)

    if case_sensitive:
        return section_type in allowed_sections
    else:
        return section_type.upper() in allowed_sections


def normalize_section_name(
    section_type: Optional[str], section_config: Optional[Dict[str, Any]]
) -> str:
    """Normalize section name based on configuration.

    Returns original section type if allowed, or "OTHER" if not.

    Args:
        section_type: BioC section type (e.g., "ABSTRACT", "INTRO")
        section_config: Section configuration dict from load_section_config(), or None

    Returns:
        Original section_type if allowed or config is None, "OTHER" otherwise.
        Returns "UNLABELED" if section_type is None or empty.
    """
    if not section_type:
        return "UNLABELED"

    if section_config is None:
        return section_type

    if is_section_allowed(section_type, section_config):
        return section_type

    return "OTHER"


def merge_consecutive_sections(
    passages_metadata: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge consecutive passages with the same section_type into unified sections.

    Args:
        passages_metadata: List of passage metadata dicts with section_type, start, end

    Returns:
        List of merged section boundaries
    """
    if not passages_metadata:
        return []

    merged_sections = []
    current_section = None

    for passage in passages_metadata:
        section_type = passage.get("section_type")

        if current_section is None:
            # Start first section
            current_section = {
                "section_type": section_type,
                "start": passage["start"],
                "end": passage["end"],
                "passage_indices": [passage["passage_index"]],
            }
        elif current_section["section_type"] == section_type:
            # Extend current section
            current_section["end"] = passage["end"]
            current_section["passage_indices"].append(passage["passage_index"])
        else:
            # Save current section and start new one
            merged_sections.append(current_section)
            current_section = {
                "section_type": section_type,
                "start": passage["start"],
                "end": passage["end"],
                "passage_indices": [passage["passage_index"]],
            }

    # Add final section
    if current_section is not None:
        merged_sections.append(current_section)

    return merged_sections


def load_bioc_document(
    bioc_file_path: str,
    document_id: str,
    section_config: Optional[Dict[str, Any]] = None,
    verbose: bool = True,
) -> DocumentData:
    """Load a BioC XML file and extract a specific document by ID.

    Args:
        bioc_file_path: Path to BioC XML file
        document_id: ID of the document to extract
        section_config: Optional section filtering configuration dict
        verbose: If True, print progress messages

    Returns:
        DocumentData object with doc_id, text, annotations, and metadata

    Raises:
        FileNotFoundError: If BioC file doesn't exist
        ValueError: If document ID not found in file
    """
    bioc_path = Path(bioc_file_path)
    if not bioc_path.exists():
        raise FileNotFoundError(f"BioC file not found: {bioc_path}")

    if verbose:
        print(f"Loading BioC file: {bioc_path}")

    with open(bioc_path, "r", encoding="utf-8") as f:
        collection = bioc_load(f)

    # Find the target document
    target_doc = None
    for doc in collection.documents:
        if doc.id == document_id:
            target_doc = doc
            break

    if target_doc is None:
        available_ids = [doc.id for doc in collection.documents[:10]]
        raise ValueError(
            f"Document ID '{document_id}' not found in BioC file. "
            f"Available IDs (first 10): {available_ids}"
        )

    if verbose:
        print(f"Found document {target_doc.id} with {len(target_doc.passages)} passages")

    # Prepare metadata scaffolding
    doc_infons = target_doc.infons.copy() if target_doc.infons else {}
    metadata_sections: List[Dict[str, Any]] = []
    metadata_info: Dict[str, Any] = {
        "pmid": doc_infons.get("article-id_pmid") or doc_infons.get("pmid"),
        "pmcid": doc_infons.get("article-id_pmc") or doc_infons.get("pmcid"),
        "doi": doc_infons.get("article-id_doi") or doc_infons.get("doi"),
        "document_type": doc_infons.get("type"),
        "date": {
            "year": doc_infons.get("year"),
            "month": doc_infons.get("month"),
            "day": doc_infons.get("day"),
        },
        "bioc_date": getattr(target_doc, "date", None),
        "passages": metadata_sections,
        "sections": [],
    }

    # Combine all passages into a single text with absolute offsets
    full_text_parts: List[str] = []
    annotations: List[Dict[str, Any]] = []
    current_length = 0

    # Sort passages by offset to ensure correct order
    sorted_passages = sorted(
        target_doc.passages, key=lambda p: p.offset if p.offset is not None else 0
    )

    for passage_idx, bioc_passage in enumerate(sorted_passages):
        passage_text = bioc_passage.text or ""
        passage_offset = (
            bioc_passage.offset if bioc_passage.offset is not None else current_length
        )

        # If passage offset is beyond current length, add padding
        if passage_offset > current_length:
            padding_length = passage_offset - current_length
            full_text_parts.append(" " * padding_length)
            current_length += padding_length
        elif passage_offset < current_length:
            # Overlapping passages - keep existing text continuity
            passage_offset = current_length

        passage_start = current_length

        if passage_text:
            full_text_parts.append(passage_text)
            current_length += len(passage_text)
        passage_end = current_length

        # Track section metadata if available
        passage_infons = bioc_passage.infons or {}
        section_type = (
            passage_infons.get("section_type")
            or passage_infons.get("section")
            or passage_infons.get("type")
        )
        # Normalize section name based on config
        normalized_section = normalize_section_name(section_type, section_config)
        if normalized_section:
            metadata_sections.append(
                {
                    "section_type": normalized_section,
                    "passage_index": passage_idx,
                    "start": passage_start,
                    "end": passage_end,
                    "length": passage_end - passage_start,
                    "passage_label": passage_infons.get("type"),
                }
            )

        # Convert passage annotations to our format
        for bioc_ann in bioc_passage.annotations:
            if bioc_ann.locations:
                for location in bioc_ann.locations:
                    ann_type = "ENTITY"
                    if bioc_ann.infons:
                        ann_type = (
                            bioc_ann.infons.get("type")
                            or bioc_ann.infons.get("identifier")
                            or bioc_ann.infons.get("label")
                            or "ENTITY"
                        )

                    annotations.append(
                        {
                            "span": {
                                "begin": location.offset,
                                "end": location.offset + location.length,
                            },
                            "obj": ann_type,
                            "text": bioc_ann.text or "",
                            "id": bioc_ann.id,
                        }
                    )

    # Combine all text parts
    full_text = "".join(full_text_parts)

    # Merge consecutive passages into unified sections
    metadata_info["sections"] = merge_consecutive_sections(metadata_sections)

    if verbose:
        print(f"Combined text length: {len(full_text)} characters")
        print(f"Total annotations: {len(annotations)}")
        print(
            f"Passages: {len(metadata_sections)}, "
            f"Merged sections: {len(metadata_info['sections'])}"
        )

    return DocumentData(
        doc_id=target_doc.id,
        text=full_text,
        annotations=annotations,
        infons=doc_infons,
        metadata_source=metadata_info,
    )


def load_bioc_collection(
    bioc_file_path: str,
    section_config: Optional[Dict[str, Any]] = None,
    verbose: bool = True,
) -> List[DocumentData]:
    """Load all documents from a BioC XML file.

    Args:
        bioc_file_path: Path to BioC XML file
        section_config: Optional section filtering configuration dict
        verbose: If True, print progress messages

    Returns:
        List of DocumentData objects

    Raises:
        FileNotFoundError: If BioC file doesn't exist
    """
    bioc_path = Path(bioc_file_path)
    if not bioc_path.exists():
        raise FileNotFoundError(f"BioC file not found: {bioc_path}")

    if verbose:
        print(f"Loading BioC collection: {bioc_path}")

    with open(bioc_path, "r", encoding="utf-8") as f:
        collection = bioc_load(f)

    documents = []
    for doc in collection.documents:
        try:
            doc_data = load_bioc_document(
                bioc_file_path,
                doc.id,
                section_config=section_config,
                verbose=False,
            )
            documents.append(doc_data)
        except Exception as e:
            if verbose:
                print(f"Warning: Failed to load document {doc.id}: {e}")

    if verbose:
        print(f"Loaded {len(documents)} documents from collection")

    return documents

