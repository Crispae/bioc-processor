"""Sentence processing and annotation mapping for Odinson conversion.

This module handles splitting text into sentences using spaCy and
mapping annotations to sentences with proper offset conversion.
"""

from typing import Any, Dict, List, Optional, Tuple

from spacy.language import Language
from spacy.tokens import Doc

from .models import Annotation, SentenceAnnotation
from .utils import format_annotations, remove_overlapping_spans

# Import clu-bridge for Odinson conversion
try:
    from clu.bridge.conversion import ConversionUtils
    from clu.bridge import processors
except ImportError as e:
    raise ImportError(
        "clu-bridge is required for Odinson conversion. "
        "Install it with: pip install git+https://github.com/clulab/clu-bridge.git "
        "or: cd clu-bridge && pip install -e ."
    ) from e


class CustomNer:
    """Custom spaCy component to add entity annotations."""

    def __init__(
        self,
        nlp: Language,
        annotations: Dict[str, Tuple[str, List[Tuple[int, int, str]]]],
    ):
        """Initialize custom NER component.

        Args:
            nlp: spaCy language model
            annotations: Dict mapping doc_id to (text, list of (start, end, label) tuples)
        """
        self.annotations = annotations

    def __call__(self, doc: Doc) -> Doc:
        """Add entity annotations to the doc."""
        doc_id = doc._.doc_id
        if doc_id in self.annotations:
            text, entities = self.annotations[doc_id]
            for start, end, label in entities:
                # Convert character offsets to token span
                span = doc.char_span(start, end, label=label)
                if span is None:
                    # Try to find approximate match if exact fails
                    continue
                doc.ents = list(doc.ents) + [span]
        return doc


def _register_custom_ner():
    """Register the CUSTOM_NER factory if not already registered."""
    from spacy.language import Language

    if "CUSTOM_NER" not in Language.factories:

        @Language.factory("CUSTOM_NER", default_config={"annotations": {}})
        def create_custom_ner(
            nlp: Language,
            name: str,
            annotations: Dict[str, Tuple[str, List[Tuple[int, int, str]]]],
        ):
            """Factory function to create CustomNer component."""
            return CustomNer(nlp, annotations)


# Register on module load
_register_custom_ner()


def create_sentence_annotations(
    text: str,
    annotations: List[Dict[str, Any]],
    nlp: Language,
    doc: Optional[Doc] = None,
) -> List[SentenceAnnotation]:
    """Split text into sentences using spaCy and map annotations to sentences.

    Args:
        text: The full text
        annotations: List of annotation dicts
        nlp: spaCy language model
        doc: Optional pre-processed spaCy Doc (will be created if not provided)

    Returns:
        List of SentenceAnnotation objects with sentence-relative offsets
    """
    # Process text with spaCy to get sentences
    doc = doc or nlp(text)

    # Format annotations
    _, entity_spans = format_annotations(text, annotations)

    # Create sentence annotations
    sentence_annotations = []

    for sent_idx, sent in enumerate(doc.sents):
        sent_start_char = sent.start_char
        sent_end_char = sent.end_char

        # Find annotations that overlap with this sentence
        sent_annotations = []
        for ann_start, ann_end, label in entity_spans:
            # Check if annotation overlaps with sentence
            if ann_start < sent_end_char and ann_end > sent_start_char:
                # Calculate relative offsets within sentence
                rel_start = max(0, ann_start - sent_start_char)
                rel_end = min(len(sent.text), ann_end - sent_start_char)

                # Create annotation relative to sentence
                sent_annotations.append(
                    Annotation(
                        begin=rel_start,
                        end=rel_end,
                        label=label,
                        text=(
                            sent.text[rel_start:rel_end]
                            if rel_start < len(sent.text)
                            else None
                        ),
                    )
                )

        sentence_annotations.append(
            SentenceAnnotation(
                text=sent.text,
                annotations=sent_annotations,
                sentence_id=f"sentence_{sent_idx}",
                abs_start=sent_start_char,
                abs_end=sent_end_char,
            )
        )

    return sentence_annotations


def process_sentence_to_odinson(
    sentence_annotation: SentenceAnnotation,
    nlp: Language,
    doc_id: str = "document",
    metadata_fields: Optional[List[Any]] = None,
) -> Any:
    """Process a single sentence with annotations and convert to Odinson format.

    Args:
        sentence_annotation: SentenceAnnotation object
        nlp: spaCy language model
        doc_id: Document ID
        metadata_fields: Optional list of Odinson metadata fields

    Returns:
        Odinson Document object (with single sentence)
    """
    text = sentence_annotation.text
    annotations = sentence_annotation.annotations

    # Create annotations dict for custom NER component
    annotations_dict = {
        doc_id: (text, [(ann.begin, ann.end, ann.label) for ann in annotations])
    }

    # Process the sentence
    doc = nlp.make_doc(text)
    doc._.doc_id = doc_id

    # Add custom NER component if not already added
    if "CUSTOM_NER" not in nlp.pipe_names:
        nlp.add_pipe("CUSTOM_NER", last=True, config={"annotations": annotations_dict})
    else:
        # Update existing component
        nlp.get_pipe("CUSTOM_NER").annotations = annotations_dict

    # Process with pipeline
    doc = nlp(doc)

    # Convert to CLU processors Document
    clu_doc = ConversionUtils.spacy.to_clu_document(doc)
    clu_doc.id = doc_id

    # Convert to Odinson Document
    odinson_doc = ConversionUtils.processors.to_odinson_document(clu_doc)
    odinson_doc.id = doc_id
    if metadata_fields is not None:
        odinson_doc.metadata = list(metadata_fields)

    return odinson_doc


def process_multiple_sentences_to_odinson(
    sentence_annotations: List[SentenceAnnotation],
    nlp: Language,
    doc_id: str = "document",
    metadata_fields: Optional[List[Any]] = None,
) -> Any:
    """Process multiple sentences and create a single Odinson document.

    Args:
        sentence_annotations: List of SentenceAnnotation objects
        nlp: spaCy language model
        doc_id: Document ID
        metadata_fields: Optional list of Odinson metadata fields

    Returns:
        Odinson Document object with multiple sentences
    """
    clu_sentences = []

    # Combine all annotations for the custom NER component
    all_annotations = {}
    for sent_idx, sent_ann in enumerate(sentence_annotations):
        sent_doc_id = f"{doc_id}_sent_{sent_idx}"
        all_annotations[sent_doc_id] = (
            sent_ann.text,
            [(ann.begin, ann.end, ann.label) for ann in sent_ann.annotations],
        )

    # Add custom NER component if not already added
    if "CUSTOM_NER" not in nlp.pipe_names:
        nlp.add_pipe("CUSTOM_NER", last=True, config={"annotations": all_annotations})
    else:
        # Update with all annotations
        nlp.get_pipe("CUSTOM_NER").annotations = all_annotations

    for sent_idx, sent_ann in enumerate(sentence_annotations):
        sent_doc_id = f"{doc_id}_sent_{sent_idx}"

        # Process the sentence
        doc = nlp.make_doc(sent_ann.text)
        doc._.doc_id = sent_doc_id

        # Process with pipeline (component will use doc_id to find annotations)
        doc = nlp(doc)

        # Convert to CLU processors Sentence
        if len(list(doc.sents)) > 0:
            sent_span = list(doc.sents)[0]
        else:
            # Fallback: use the entire doc as a sentence span
            sent_span = doc[:]
        clu_sentence = ConversionUtils.spacy.to_clu_sentence(sent_span)
        clu_sentences.append(clu_sentence)

    # Create CLU processors Document
    clu_doc = processors.Document(id=doc_id, sentences=clu_sentences)

    # Convert to Odinson Document
    odinson_doc = ConversionUtils.processors.to_odinson_document(clu_doc)
    odinson_doc.id = doc_id
    if metadata_fields is not None:
        odinson_doc.metadata = list(metadata_fields)

    return odinson_doc


def process_annotations_to_odinson(
    text: str,
    annotations: List[Dict[str, Any]],
    nlp: Language,
    doc_id: str = "document",
    combine_sentences: bool = True,
    metadata_fields: Optional[List[Any]] = None,
) -> Any:
    """Main function to process text with annotations and convert to Odinson format.

    This is the primary function to use for converting annotated text to Odinson.

    Args:
        text: The full text
        annotations: List of annotation dicts with 'span' (begin, end) and 'obj' (label)
        nlp: spaCy language model (should have doc_id extension set)
        doc_id: Document ID
        combine_sentences: If True, create one Odinson doc with all sentences;
                          If False, return list of Odinson docs (one per sentence)
        metadata_fields: Optional list of Odinson metadata fields to attach

    Returns:
        If combine_sentences=True: Single Odinson Document
        If combine_sentences=False: List of Odinson Documents
    """
    # Format annotations and remove overlaps
    text_clean, _ = format_annotations(text, annotations)

    # Process the FULL text with spaCy first to get proper lemmatization
    # This ensures context is preserved and lemmatization works correctly
    full_doc = nlp(text_clean)

    # Create sentence annotations
    sentence_annotations = create_sentence_annotations(
        text_clean,
        annotations,
        nlp,
        doc=full_doc,
    )

    if combine_sentences:
        # Return single Odinson document with all sentences
        return process_multiple_sentences_to_odinson(
            sentence_annotations,
            nlp,
            doc_id,
            metadata_fields=metadata_fields,
        )
    else:
        # Return list of Odinson documents (one per sentence)
        odinson_docs = []
        for sent_ann in sentence_annotations:
            sent_doc_id = (
                f"{doc_id}_{sent_ann.sentence_id}" if sent_ann.sentence_id else doc_id
            )
            odinson_doc = process_sentence_to_odinson(
                sent_ann,
                nlp,
                sent_doc_id,
                metadata_fields=metadata_fields,
            )
            odinson_docs.append(odinson_doc)
        return odinson_docs


def setup_nlp_for_conversion(nlp: Language) -> Language:
    """Setup a spaCy language model for Odinson conversion.

    This function prepares the NLP model by:
    1. Setting up the doc_id extension
    2. Removing the default NER component if present

    Args:
        nlp: spaCy language model

    Returns:
        Configured spaCy language model
    """
    # Set extension to doc object if not already set
    if not Doc.has_extension("doc_id"):
        Doc.set_extension("doc_id", default=None)

    # Remove default NER if present (to use custom annotations)
    if "ner" in nlp.pipe_names:
        nlp.remove_pipe("ner")

    return nlp

