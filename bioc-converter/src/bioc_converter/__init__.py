"""bioc-converter: Convert BioC XML documents to Odinson JSON format.

This library provides tools for converting BioC XML documents (commonly used
in biomedical text mining) to Odinson format (used for pattern-based information
extraction).

Basic Usage:
    >>> import spacy
    >>> from bioc_converter import convert_bioc_to_odinson, save_odinson_json
    >>>
    >>> # Load spaCy model
    >>> nlp = spacy.load("en_core_sci_sm")
    >>>
    >>> # Convert BioC to Odinson
    >>> odinson_doc = convert_bioc_to_odinson(
    ...     bioc_file_path="document.bioc.xml",
    ...     document_id="12345",
    ...     nlp=nlp
    ... )
    >>>
    >>> # Save output
    >>> save_odinson_json(odinson_doc, "output.json")

For section-based processing (one Odinson doc per section):
    >>> from bioc_converter import process_bioc_by_sections, save_odinson_sections
    >>>
    >>> # Process by sections
    >>> odinson_docs = process_bioc_by_sections(
    ...     "document.bioc.xml", "12345", nlp
    ... )
    >>> # Returns: {"TITLE": OdinsonDoc, "ABSTRACT": OdinsonDoc, ...}
    >>>
    >>> # Save each section as separate JSON
    >>> save_odinson_sections(odinson_docs, "output/", "12345", compress=True)

For converting plain text with annotations:
    >>> from bioc_converter import convert_text_to_odinson
    >>>
    >>> text = "The patient has diabetes."
    >>> annotations = [{"span": {"begin": 16, "end": 24}, "obj": "DISEASE"}]
    >>> odinson_doc = convert_text_to_odinson(text, annotations, nlp)
"""

from .__version__ import __version__, __version_info__

# Main conversion functions
from .converter import (
    convert_bioc_to_odinson,
    convert_text_to_odinson,
    save_odinson_json,
    save_odinson_sections,
)

# Section-based processing (notebook workflow)
from .sections import (
    process_bioc_by_sections,
    group_passages_by_section,
    process_passage_to_sentences,
    create_odinson_doc_from_sentences,
)

# Document loading
from .loader import (
    load_bioc_document,
    load_bioc_collection,
    load_section_config,
)

# Data models
from .models import (
    Annotation,
    SentenceAnnotation,
    DocumentData,
    SectionInfo,
)

# Sentence processing
from .sentence import (
    create_sentence_annotations,
    process_annotations_to_odinson,
    setup_nlp_for_conversion,
)

# Metadata utilities
from .metadata import (
    build_odinson_metadata_fields,
    group_sentence_annotations_by_section,
)

__all__ = [
    # Version
    "__version__",
    "__version_info__",
    # Main API
    "convert_bioc_to_odinson",
    "convert_text_to_odinson",
    "save_odinson_json",
    "save_odinson_sections",
    # Section-based processing (notebook workflow)
    "process_bioc_by_sections",
    "group_passages_by_section",
    "process_passage_to_sentences",
    "create_odinson_doc_from_sentences",
    # Loading
    "load_bioc_document",
    "load_bioc_collection",
    "load_section_config",
    # Models
    "Annotation",
    "SentenceAnnotation",
    "DocumentData",
    "SectionInfo",
    # Sentence processing
    "create_sentence_annotations",
    "process_annotations_to_odinson",
    "setup_nlp_for_conversion",
    # Metadata
    "build_odinson_metadata_fields",
    "group_sentence_annotations_by_section",
]

