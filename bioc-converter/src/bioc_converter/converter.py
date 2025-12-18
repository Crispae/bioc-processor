"""Main conversion functions for BioC to Odinson format.

This module provides the high-level API for converting BioC documents
to Odinson format.
"""

import json
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from spacy.language import Language
from spacy.tokens import Doc

from .loader import load_bioc_document, load_section_config
from .metadata import (
    build_odinson_metadata_fields,
    group_sentence_annotations_by_section,
)
from .models import DocumentData
from .sentence import (
    create_sentence_annotations,
    process_annotations_to_odinson,
    process_multiple_sentences_to_odinson,
    setup_nlp_for_conversion,
)
from .utils import make_random_id, sanitize_section_name


def convert_bioc_to_odinson(
    bioc_file_path: str,
    document_id: str,
    nlp: Language,
    combine_sentences: bool = True,
    doc_data: Optional[Union[Dict[str, Any], DocumentData]] = None,
    group_by_section: bool = False,
    section_config: Optional[Dict[str, Any]] = None,
    section_config_path: Optional[str] = None,
    verbose: bool = True,
) -> Any:
    """Convert a BioC document to Odinson format.

    This is the main entry point for converting BioC documents to Odinson.

    Args:
        bioc_file_path: Path to BioC XML file (ignored if doc_data is provided)
        document_id: ID of the document to convert (ignored if doc_data is provided)
        nlp: spaCy language model (should have doc_id extension set)
        combine_sentences: If True, create one Odinson doc with all sentences
        doc_data: Optional pre-loaded document data (skips BioC loading if provided)
        group_by_section: If True, create separate documents per section
        section_config: Optional section filtering configuration dict
        section_config_path: Optional path to section config JSON file
        verbose: If True, print progress messages

    Returns:
        Single Odinson Document, or a list of documents when either
        combine_sentences=False or group_by_section=True

    Example:
        >>> import spacy
        >>> from bioc_converter import convert_bioc_to_odinson
        >>> nlp = spacy.load("en_core_sci_sm")
        >>> odinson_doc = convert_bioc_to_odinson(
        ...     "document.bioc.xml",
        ...     "12345",
        ...     nlp,
        ...     combine_sentences=True
        ... )
    """
    # Ensure NLP model is set up correctly
    nlp = setup_nlp_for_conversion(nlp)

    # Load section config from file if path provided
    if section_config is None and section_config_path:
        section_config = load_section_config(section_config_path)

    # Load BioC document if not provided
    if doc_data is None:
        doc_data = load_bioc_document(
            bioc_file_path,
            document_id,
            section_config=section_config,
            verbose=verbose,
        )

    # Convert DocumentData to dict if needed
    if isinstance(doc_data, DocumentData):
        doc_data_dict = doc_data.to_dict()
    else:
        doc_data_dict = doc_data

    # Build metadata fields
    metadata_fields = build_odinson_metadata_fields(doc_data_dict)

    # Convert to Odinson format
    if group_by_section:
        sentence_annotations = create_sentence_annotations(
            doc_data_dict["text"],
            doc_data_dict["annotations"],
            nlp,
        )
        sections_info = (
            doc_data_dict.get("metadata_source", {}).get("sections") or []
        )
        grouped = group_sentence_annotations_by_section(
            sentence_annotations, sections_info
        )
        odinson_docs = []
        for section_name, sentences in grouped.items():
            if not sentences:
                continue
            section_label = section_name or "UNLABELED"
            section_doc_id = make_random_id()
            section_metadata = build_odinson_metadata_fields(
                doc_data_dict, section_name=section_label
            )
            doc = process_multiple_sentences_to_odinson(
                sentences,
                nlp,
                doc_id=section_doc_id,
                metadata_fields=section_metadata,
            )
            odinson_docs.append(doc)
        return odinson_docs

    if verbose:
        print(
            f"\nConverting to Odinson format (combine_sentences={combine_sentences})..."
        )

    odinson_doc = process_annotations_to_odinson(
        doc_data_dict["text"],
        doc_data_dict["annotations"],
        nlp,
        doc_id=make_random_id(),
        combine_sentences=combine_sentences,
        metadata_fields=metadata_fields,
    )

    return odinson_doc


def save_odinson_json(
    odinson_doc: Any,
    output_path: str,
    compress_multiple: bool = False,
    verbose: bool = True,
) -> None:
    """Save Odinson document to JSON file.

    Args:
        odinson_doc: Odinson Document object or list of documents
        output_path: Path to output JSON file
        compress_multiple: If True and odinson_doc is a list, save each section as
                          separate JSON file and compress all into a single tar.gz archive
        verbose: If True, print progress messages

    Example:
        >>> save_odinson_json(odinson_doc, "output.json")
        >>> # For multiple sections with compression:
        >>> save_odinson_json(odinson_docs, "output.json", compress_multiple=True)
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    def _write_doc(doc_obj: Any, path: Path) -> Path:
        try:
            json_output = doc_obj.model_dump_json(indent=2, by_alias=True)
        except AttributeError:
            json_output = doc_obj.json(by_alias=True, indent=2)

        with open(path, "w", encoding="utf-8") as f:
            f.write(json_output)
        if verbose:
            print(f"  Saved: {path.name}")
        return path

    if isinstance(odinson_doc, list):
        if compress_multiple:
            _save_compressed_sections(odinson_doc, output_file, verbose)
        else:
            _save_separate_sections(odinson_doc, output_file, verbose)
    else:
        _write_doc(odinson_doc, output_file)
        if verbose:
            file_size = output_file.stat().st_size / 1024
            print(f"File size: {file_size:.2f} KB")


def _save_compressed_sections(
    odinson_docs: List[Any],
    output_file: Path,
    verbose: bool = True,
) -> None:
    """Save multiple Odinson documents as compressed tar.gz archive."""
    # Get doc_id from first document for filename
    doc_id_value = _extract_metadata_value(odinson_docs[0], "doc_id") if odinson_docs else None

    # Create temporary directory for JSON files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        json_files = []

        stem = output_file.stem or "odinson"
        for doc in odinson_docs:
            section_value = _extract_metadata_value(doc, "section")
            if not section_value:
                section_value = doc.id.split("_")[-1] if hasattr(doc, "id") else "unknown"

            section_suffix = sanitize_section_name(section_value)
            doc_id_suffix = sanitize_section_name(doc_id_value) if doc_id_value else None

            if doc_id_suffix:
                file_name = f"{stem}_{doc_id_suffix}_{section_suffix}.json"
            else:
                file_name = f"{stem}_{section_suffix}.json"

            json_path = temp_path / file_name
            _write_single_doc(doc, json_path)
            json_files.append(json_path)

        # Create tar.gz archive
        if doc_id_value:
            doc_id_suffix = sanitize_section_name(doc_id_value)
            archive_name = f"{stem}_{doc_id_suffix}_sections.tar.gz"
        else:
            archive_name = f"{stem}_sections.tar.gz"
        archive_path = output_file.with_name(archive_name)

        with tarfile.open(archive_path, "w:gz") as tar:
            for json_file in json_files:
                tar.add(json_file, arcname=json_file.name)

        if verbose:
            print(
                f"\nSaved {len(odinson_docs)} section JSON files compressed into: "
                f"{archive_path}"
            )
            file_size = archive_path.stat().st_size / 1024
            print(f"Archive size: {file_size:.2f} KB (compressed)")


def _save_separate_sections(
    odinson_docs: List[Any],
    output_file: Path,
    verbose: bool = True,
) -> None:
    """Save multiple Odinson documents as separate JSON files."""
    stem = output_file.stem or "odinson"
    suffix = output_file.suffix or ".json"

    for doc in odinson_docs:
        section_value = _extract_metadata_value(doc, "section")
        if not section_value:
            section_value = doc.id.split("_")[-1] if hasattr(doc, "id") else "unknown"

        doc_id_value = _extract_metadata_value(doc, "doc_id")
        section_suffix = sanitize_section_name(section_value)
        doc_id_suffix = sanitize_section_name(doc_id_value) if doc_id_value else None

        if doc_id_suffix:
            file_name = f"{stem}_{doc_id_suffix}_{section_suffix}{suffix}"
        else:
            file_name = f"{stem}_{section_suffix}{suffix}"

        section_path = output_file.with_name(file_name)
        _write_single_doc(doc, section_path)

        if verbose:
            file_size = section_path.stat().st_size / 1024
            print(f"File size: {file_size:.2f} KB")


def _write_single_doc(doc_obj: Any, path: Path) -> None:
    """Write a single Odinson document to a file."""
    try:
        json_output = doc_obj.model_dump_json(indent=2, by_alias=True)
    except AttributeError:
        json_output = doc_obj.json(by_alias=True, indent=2)

    with open(path, "w", encoding="utf-8") as f:
        f.write(json_output)


def _extract_metadata_value(doc: Any, field_name: str) -> Optional[str]:
    """Extract a metadata field value from an Odinson document."""
    if not hasattr(doc, "metadata"):
        return None

    field = next((f for f in doc.metadata if f.name == field_name), None)
    if field and hasattr(field, "tokens") and field.tokens:
        return field.tokens[0]
    return None


def convert_text_to_odinson(
    text: str,
    annotations: List[Dict[str, Any]],
    nlp: Language,
    doc_id: str = "document",
    combine_sentences: bool = True,
    metadata_fields: Optional[List[Any]] = None,
) -> Any:
    """Convert plain text with annotations directly to Odinson format.

    This is a convenience function for when you have text and annotations
    but not a BioC file.

    Args:
        text: The full text
        annotations: List of annotation dicts with 'span' (begin, end) and 'obj' (label)
        nlp: spaCy language model
        doc_id: Document ID
        combine_sentences: If True, create one Odinson doc with all sentences
        metadata_fields: Optional list of Odinson metadata fields

    Returns:
        Odinson Document (or list if combine_sentences=False)

    Example:
        >>> text = "The patient has diabetes. Treatment includes insulin."
        >>> annotations = [
        ...     {"span": {"begin": 16, "end": 24}, "obj": "DISEASE"},
        ...     {"span": {"begin": 46, "end": 53}, "obj": "CHEMICAL"},
        ... ]
        >>> odinson_doc = convert_text_to_odinson(text, annotations, nlp)
    """
    nlp = setup_nlp_for_conversion(nlp)

    return process_annotations_to_odinson(
        text,
        annotations,
        nlp,
        doc_id=doc_id,
        combine_sentences=combine_sentences,
        metadata_fields=metadata_fields,
    )


def save_odinson_sections(
    odinson_docs: Dict[str, Any],
    folder_path: str,
    doc_name: str,
    compress: bool = False,
    verbose: bool = True,
) -> List[Path]:
    """Save Odinson documents from a dictionary to separate JSON files per section.

    This function is designed to work with the output of process_bioc_by_sections(),
    which returns a dictionary mapping section names to Odinson documents.

    Args:
        odinson_docs: Dictionary mapping section names to Odinson Document objects
                     e.g., {"TITLE": OdinsonDoc, "ABSTRACT": OdinsonDoc, ...}
        folder_path: Path to folder where JSON files will be saved
        doc_name: Base name for files (e.g., document ID)
        compress: If True, also create a tar.gz archive with all JSON files
        verbose: If True, print progress messages

    Returns:
        List of saved file paths

    Example:
        >>> from bioc_converter import process_bioc_by_sections, save_odinson_sections
        >>> odinson_docs = process_bioc_by_sections("doc.bioc.xml", "12345", nlp)
        >>> saved_files = save_odinson_sections(
        ...     odinson_docs,
        ...     folder_path="output/",
        ...     doc_name="12345",
        ...     compress=True
        ... )
        Saved: 12345_title.json (1 sentences)
        Saved: 12345_abstract.json (9 sentences)
        ...
        Created archive: 12345_sections.tar.gz (167.25 KB)
    """
    import re

    output_dir = Path(folder_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    saved_files: List[Path] = []

    for section_name, odinson_doc in odinson_docs.items():
        # Sanitize section name for filename
        section_safe = re.sub(r"[^a-z0-9]+", "_", section_name.lower()).strip("_")
        section_safe = section_safe or "section"

        filename = f"{doc_name}_{section_safe}.json"
        file_path = output_dir / filename

        # Serialize to JSON
        try:
            json_output = odinson_doc.model_dump_json(indent=2, by_alias=True)
        except AttributeError:
            json_output = odinson_doc.json(by_alias=True, indent=2)

        # Write to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json_output)

        saved_files.append(file_path)

        if verbose:
            num_sentences = len(odinson_doc.sentences) if hasattr(odinson_doc, "sentences") else 0
            print(f"Saved: {file_path.name} ({num_sentences} sentences)")

    # Create tar.gz archive if requested
    if compress and saved_files:
        archive_name = f"{doc_name}_sections.tar.gz"
        archive_path = output_dir / archive_name

        with tarfile.open(archive_path, "w:gz") as tar:
            for json_file in saved_files:
                tar.add(json_file, arcname=json_file.name)

        if verbose:
            archive_size = archive_path.stat().st_size / 1024
            print(f"\nCreated archive: {archive_name} ({archive_size:.2f} KB)")

    if verbose:
        print(f"\nSaved {len(saved_files)} section files to: {output_dir}")

    return saved_files

