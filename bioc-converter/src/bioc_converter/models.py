"""Data models for bioc-converter.

This module contains the core data classes used throughout the conversion process.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Annotation:
    """Represents an entity annotation with span and label.

    Attributes:
        begin: Character offset start (0-based)
        end: Character offset end (exclusive)
        label: Entity type/label (e.g., "DISEASE", "CHEMICAL")
        text: Optional text content of the annotation
        id: Optional annotation ID from the source document
        infons: Optional additional metadata from BioC
    """

    begin: int
    end: int
    label: str
    text: Optional[str] = None
    id: Optional[str] = None
    infons: Optional[Dict[str, Any]] = None

    @property
    def length(self) -> int:
        """Return the length of the annotation span."""
        return self.end - self.begin

    def to_dict(self) -> Dict[str, Any]:
        """Convert annotation to dictionary format."""
        return {
            "span": {"begin": self.begin, "end": self.end},
            "obj": self.label,
            "text": self.text,
            "id": self.id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Annotation":
        """Create an Annotation from a dictionary.

        Supports multiple input formats:
        - {"span": {"begin": 0, "end": 10}, "obj": "LABEL"}
        - {"begin": 0, "end": 10, "label": "LABEL"}
        - {"start": 0, "end": 10, "type": "LABEL"}
        """
        # Extract span information
        if "span" in data:
            span = data["span"]
            begin = span.get("begin", span.get("start", 0))
            end = span.get("end", begin + span.get("length", 0))
        else:
            begin = data.get("begin", data.get("start", 0))
            end = data.get("end", begin + data.get("length", 0))

        # Extract label
        label = data.get("obj", data.get("label", data.get("type", "ENTITY")))

        return cls(
            begin=begin,
            end=end,
            label=label,
            text=data.get("text"),
            id=data.get("id"),
            infons=data.get("infons"),
        )


@dataclass
class SentenceAnnotation:
    """Represents a sentence with its associated annotations.

    This class holds a sentence extracted from the document along with
    any entity annotations that fall within it. Offsets in annotations
    are relative to the sentence start.

    Attributes:
        text: The sentence text
        annotations: List of Annotation objects with sentence-relative offsets
        sentence_id: Optional identifier for this sentence
        abs_start: Absolute character offset where sentence starts in original document
        abs_end: Absolute character offset where sentence ends in original document
    """

    text: str
    annotations: List[Annotation] = field(default_factory=list)
    sentence_id: Optional[str] = None
    abs_start: Optional[int] = None
    abs_end: Optional[int] = None

    @property
    def length(self) -> int:
        """Return the length of the sentence text."""
        return len(self.text)

    @property
    def has_annotations(self) -> bool:
        """Check if sentence has any annotations."""
        return len(self.annotations) > 0

    def get_annotation_tuples(self) -> List[tuple]:
        """Return annotations as list of (begin, end, label) tuples."""
        return [(ann.begin, ann.end, ann.label) for ann in self.annotations]


@dataclass
class DocumentData:
    """Represents a loaded BioC document ready for conversion.

    Attributes:
        doc_id: Document identifier
        text: Combined full text from all passages
        annotations: List of annotation dictionaries with absolute offsets
        infons: Document-level metadata from BioC
        metadata_source: Extracted metadata including sections, dates, etc.
    """

    doc_id: str
    text: str
    annotations: List[Dict[str, Any]] = field(default_factory=list)
    infons: Dict[str, Any] = field(default_factory=dict)
    metadata_source: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for compatibility."""
        return {
            "doc_id": self.doc_id,
            "text": self.text,
            "annotations": self.annotations,
            "infons": self.infons,
            "metadata_source": self.metadata_source,
        }


@dataclass
class SectionInfo:
    """Information about a document section.

    Attributes:
        section_type: Type of section (e.g., "ABSTRACT", "METHODS")
        start: Start character offset
        end: End character offset
        passage_indices: List of passage indices that belong to this section
    """

    section_type: Optional[str]
    start: int
    end: int
    passage_indices: List[int] = field(default_factory=list)

    @property
    def length(self) -> int:
        """Return the length of the section."""
        return self.end - self.start

    def contains_offset(self, offset: int) -> bool:
        """Check if an offset falls within this section."""
        return self.start <= offset < self.end
