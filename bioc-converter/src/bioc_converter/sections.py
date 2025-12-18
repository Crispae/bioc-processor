"""Section-based processing for BioC to Odinson conversion.

This module provides functions for processing BioC documents by sections,
matching the workflow from the BioC_2_odinson.ipynb notebook.

The key difference from the standard conversion approach:
- Groups passages by section_type FIRST
- Processes each section's passages separately
- Creates one Odinson document per section with section metadata
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from spacy.language import Language
from spacy.tokens import Doc

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

# Import clu-bridge for Odinson conversion
try:
    from clu.bridge.conversion import ConversionUtils
    from clu.bridge import odinson
    from clu.bridge import processors
except ImportError as e:
    raise ImportError(
        "clu-bridge is required for Odinson conversion. "
        "Install it with: pip install git+https://github.com/clulab/clu-bridge.git "
        "or: cd clu-bridge && pip install -e ."
    ) from e

from .sentence import _register_custom_ner, CustomNer


def group_passages_by_section(bioc_document: Any) -> Dict[str, List[Any]]:
    """Group BioC passages by their section_type.

    This matches the notebook approach of grouping passages first,
    before any NLP processing.

    Args:
        bioc_document: A BioC Document object

    Returns:
        Dictionary mapping section names to lists of passage objects
        e.g., {"TITLE": [passage1], "ABSTRACT": [passage2, passage3], ...}
    """
    sections: Dict[str, List[Any]] = {}

    for passage in bioc_document.passages:
        # Get section type from passage infons
        section_type = None
        if passage.infons:
            section_type = (
                passage.infons.get("section_type")
                or passage.infons.get("section")
                or passage.infons.get("type")
            )

        # Default to UNKNOWN if no section type found
        if not section_type:
            section_type = "UNKNOWN"

        # Add passage to section group
        if section_type not in sections:
            sections[section_type] = []
        sections[section_type].append(passage)

    return sections


def process_passage_to_sentences(
    passage_text: str,
    passage_annotations: List[Any],
    passage_offset: int,
    nlp: Language,
) -> List[Dict[str, Any]]:
    """Process a single passage into sentences with mapped annotations.

    This function takes one passage's text and annotations, splits into
    sentences using spaCy, and returns sentence data with annotations
    mapped to sentence-relative offsets.

    Args:
        passage_text: The text content of the passage
        passage_annotations: List of BioC annotation objects
        passage_offset: The character offset of the passage in the document
        nlp: spaCy language model

    Returns:
        List of sentence dictionaries with:
        - text: sentence text
        - abs_start: absolute start offset
        - abs_end: absolute end offset
        - annotations: list of annotation dicts with sentence-relative offsets
    """
    if not passage_text or not passage_text.strip():
        return []

    # Process passage with spaCy to get sentence boundaries
    doc = nlp(passage_text)

    processed_sentences = []

    for sent_idx, sent in enumerate(doc.sents):
        sent_text = sent.text
        sent_start_char = sent.start_char  # Relative to passage start
        sent_end_char = sent.end_char  # Relative to passage start

        # Absolute offsets (relative to document start)
        abs_sent_start = passage_offset + sent_start_char
        abs_sent_end = passage_offset + sent_end_char

        # Find annotations that belong to THIS sentence
        sent_anns = []
        for ann in passage_annotations:
            if not ann.locations:
                continue

            loc = ann.locations[0]
            ann_abs_start = loc.offset
            ann_abs_end = loc.offset + loc.length

            # Check overlap: Does annotation overlap with this sentence's absolute span?
            if ann_abs_start < abs_sent_end and ann_abs_end > abs_sent_start:
                # Calculate offsets relative to the SENTENCE
                rel_start = max(0, ann_abs_start - abs_sent_start)
                rel_end = min(len(sent_text), ann_abs_end - abs_sent_start)

                # Get annotation type/label
                ann_type = "ENTITY"
                if ann.infons:
                    ann_type = (
                        ann.infons.get("type")
                        or ann.infons.get("identifier")
                        or ann.infons.get("label")
                        or "ENTITY"
                    )

                sent_anns.append(
                    {
                        "start": rel_start,
                        "end": rel_end,
                        "label": ann_type,
                        "text": ann.text,
                    }
                )

        # Store processed sentence data
        processed_sentences.append(
            {
                "text": sent_text,
                "abs_start": abs_sent_start,
                "abs_end": abs_sent_end,
                "annotations": sent_anns,
            }
        )

    return processed_sentences


def create_odinson_doc_from_sentences(
    sentences_data: List[Dict[str, Any]],
    doc_id: str,
    metadata_fields: List[Any],
    nlp: Language,
) -> Any:
    """Convert list of processed sentence data into a single Odinson Document.

    This matches the notebook's create_odinson_doc_from_sentences function.

    Args:
        sentences_data: List of sentence dictionaries from process_passage_to_sentences
        doc_id: Document ID for this Odinson document
        metadata_fields: List of Odinson metadata field objects
        nlp: spaCy language model

    Returns:
        Odinson Document object
    """
    # Ensure doc_id extension is set
    if not Doc.has_extension("doc_id"):
        Doc.set_extension("doc_id", default=None)

    # Prepare annotations dictionary for CustomNer
    custom_ner_data = {}
    for i, sent_data in enumerate(sentences_data):
        sent_id = f"{doc_id}_sent_{i}"
        spans = [(a["start"], a["end"], a["label"]) for a in sent_data["annotations"]]
        custom_ner_data[sent_id] = (sent_data["text"], spans)

    # Register custom NER factory if needed
    _register_custom_ner()

    # Add or update CustomNer component
    if "CUSTOM_NER" not in nlp.pipe_names:
        nlp.add_pipe("CUSTOM_NER", last=True, config={"annotations": custom_ner_data})
    else:
        nlp.get_pipe("CUSTOM_NER").annotations = custom_ner_data

    # Process sentences to create CLU sentences
    clu_sentences = []
    for i, sent_data in enumerate(sentences_data):
        sent_id = f"{doc_id}_sent_{i}"
        doc = nlp.make_doc(sent_data["text"])
        doc._.doc_id = sent_id
        doc = nlp(doc)  # Run pipeline

        # Convert to CLU document and extract sentences
        clu_doc = ConversionUtils.spacy.to_clu_document(doc)
        if clu_doc.sentences:
            clu_sentences.extend(clu_doc.sentences)

    # Create final Odinson Document
    # Use a dummy CLU doc to hold the list of sentences
    dummy_doc = processors.Document(id=doc_id, sentences=clu_sentences)

    odinson_doc = ConversionUtils.processors.to_odinson_document(dummy_doc)
    odinson_doc.id = doc_id
    odinson_doc.metadata = list(metadata_fields)

    return odinson_doc


def process_bioc_by_sections(
    bioc_file_path: str,
    document_id: str,
    nlp: Language,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Process a BioC document by sections, creating one Odinson doc per section.

    This function matches the notebook workflow:
    1. Load BioC document
    2. Group passages by section_type
    3. Process each section's passages separately
    4. Create Odinson document per section with doc_id + section metadata

    Args:
        bioc_file_path: Path to BioC XML file
        document_id: ID of the document to process
        nlp: spaCy language model
        verbose: If True, print progress messages

    Returns:
        Dictionary mapping section names to Odinson Document objects
        e.g., {"TITLE": OdinsonDoc, "ABSTRACT": OdinsonDoc, ...}

    Example:
        >>> import spacy
        >>> from bioc_converter import process_bioc_by_sections
        >>> nlp = spacy.load("en_core_sci_sm")
        >>> odinson_docs = process_bioc_by_sections(
        ...     "document.bioc.xml",
        ...     "12345",
        ...     nlp
        ... )
        >>> print(odinson_docs.keys())
        dict_keys(['TITLE', 'ABSTRACT', 'INTRO', ...])
    """
    # Ensure NLP model is set up
    if not Doc.has_extension("doc_id"):
        Doc.set_extension("doc_id", default=None)
    if "ner" in nlp.pipe_names:
        nlp.remove_pipe("ner")

    # Load BioC file
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

    # Group passages by section
    sections = group_passages_by_section(target_doc)

    if verbose:
        print(f"Found {len(sections)} sections: {list(sections.keys())}")

    # Get doc_id from document or passage metadata
    pmid = None
    if target_doc.infons:
        pmid = (
            target_doc.infons.get("article-id_pmid")
            or target_doc.infons.get("pmid")
            or target_doc.id
        )
    if not pmid:
        # Try to get from TITLE passage
        if "TITLE" in sections and sections["TITLE"]:
            title_passage = sections["TITLE"][0]
            if title_passage.infons:
                pmid = title_passage.infons.get("article-id_pmid", target_doc.id)
    pmid = pmid or target_doc.id

    # Process each section
    odinson_docs: Dict[str, Any] = {}

    for section_name, section_passages in sections.items():
        if verbose:
            print(f"Processing section: {section_name}...")

        # Collect all sentences from all passages in this section
        all_section_sentences = []

        for passage in section_passages:
            passage_text = passage.text or ""
            passage_offset = passage.offset if passage.offset is not None else 0

            sents = process_passage_to_sentences(
                passage_text=passage_text,
                passage_annotations=passage.annotations,
                passage_offset=passage_offset,
                nlp=nlp,
            )
            all_section_sentences.extend(sents)

        # Skip empty sections
        if not all_section_sentences:
            if verbose:
                print(f"  Skipping empty section: {section_name}")
            continue

        # Create Odinson metadata with doc_id and section
        metadata = [
            odinson.TokensField(name="doc_id", tokens=[str(pmid)]),
            odinson.TokensField(name="section", tokens=[section_name]),
        ]

        # Create Odinson Document for this section
        section_doc_id = f"{pmid}_{section_name}"
        odinson_doc = create_odinson_doc_from_sentences(
            sentences_data=all_section_sentences,
            doc_id=section_doc_id,
            metadata_fields=metadata,
            nlp=nlp,
        )

        odinson_docs[section_name] = odinson_doc

        if verbose:
            print(f"  Created Odinson doc with {len(odinson_doc.sentences)} sentences")

    if verbose:
        print(f"\nProcessed {len(odinson_docs)} sections")

    return odinson_docs

