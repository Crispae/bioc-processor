"""Tests for bioc-converter."""

import json
import tempfile
from pathlib import Path

import pytest


class TestModels:
    """Tests for data models."""

    def test_annotation_creation(self):
        """Test Annotation dataclass creation."""
        from bioc_converter.models import Annotation

        ann = Annotation(begin=0, end=10, label="DISEASE")
        assert ann.begin == 0
        assert ann.end == 10
        assert ann.label == "DISEASE"
        assert ann.length == 10

    def test_annotation_from_dict(self):
        """Test Annotation.from_dict with various formats."""
        from bioc_converter.models import Annotation

        # Standard format
        data1 = {"span": {"begin": 5, "end": 15}, "obj": "CHEMICAL"}
        ann1 = Annotation.from_dict(data1)
        assert ann1.begin == 5
        assert ann1.end == 15
        assert ann1.label == "CHEMICAL"

        # Alternative format
        data2 = {"begin": 10, "end": 20, "label": "GENE"}
        ann2 = Annotation.from_dict(data2)
        assert ann2.begin == 10
        assert ann2.end == 20
        assert ann2.label == "GENE"

    def test_annotation_to_dict(self):
        """Test Annotation.to_dict."""
        from bioc_converter.models import Annotation

        ann = Annotation(begin=0, end=8, label="DISEASE", text="diabetes")
        d = ann.to_dict()
        assert d["span"]["begin"] == 0
        assert d["span"]["end"] == 8
        assert d["obj"] == "DISEASE"
        assert d["text"] == "diabetes"

    def test_sentence_annotation(self):
        """Test SentenceAnnotation dataclass."""
        from bioc_converter.models import Annotation, SentenceAnnotation

        sent = SentenceAnnotation(
            text="The patient has diabetes.",
            annotations=[Annotation(begin=16, end=24, label="DISEASE")],
            sentence_id="sent_0",
            abs_start=0,
            abs_end=25,
        )
        assert sent.length == 25
        assert sent.has_annotations is True
        assert len(sent.get_annotation_tuples()) == 1

    def test_document_data(self):
        """Test DocumentData dataclass."""
        from bioc_converter.models import DocumentData

        doc = DocumentData(
            doc_id="12345",
            text="Sample text",
            annotations=[{"span": {"begin": 0, "end": 6}, "obj": "ENTITY"}],
        )
        assert doc.doc_id == "12345"

        d = doc.to_dict()
        assert d["doc_id"] == "12345"
        assert d["text"] == "Sample text"


class TestUtils:
    """Tests for utility functions."""

    def test_safe_int(self):
        """Test safe_int conversion."""
        from bioc_converter.utils import safe_int

        assert safe_int(42) == 42
        assert safe_int("42") == 42
        assert safe_int(None) is None
        assert safe_int("invalid") is None

    def test_format_date_string(self):
        """Test date string formatting."""
        from bioc_converter.utils import format_date_string

        assert format_date_string({"year": 2024}) == "2024"
        assert format_date_string({"year": 2024, "month": 3}) == "2024-03"
        assert format_date_string({"year": 2024, "month": 3, "day": 15}) == "2024-03-15"
        assert format_date_string({}) is None
        assert format_date_string(None) is None

    def test_sanitize_section_name(self):
        """Test section name sanitization."""
        from bioc_converter.utils import sanitize_section_name

        assert sanitize_section_name("ABSTRACT") == "abstract"
        assert sanitize_section_name("Results & Discussion") == "results_discussion"
        assert sanitize_section_name("") == "section"

    def test_remove_overlapping_spans(self):
        """Test overlapping span removal."""
        from bioc_converter.utils import remove_overlapping_spans

        spans = [(0, 10, "A"), (5, 15, "B"), (20, 30, "C")]
        result = remove_overlapping_spans(spans)
        assert len(result) == 2
        assert result[0] == (0, 10, "A")
        assert result[1] == (20, 30, "C")

    def test_format_annotations(self):
        """Test annotation formatting."""
        from bioc_converter.utils import format_annotations

        text = "Sample text"
        annotations = [
            {"span": {"begin": 0, "end": 6}, "obj": "WORD"},
            {"span": {"begin": 7, "end": 11}, "obj": "WORD"},
        ]
        result_text, result_spans = format_annotations(text, annotations)
        assert result_text == text
        assert len(result_spans) == 2


class TestLoader:
    """Tests for BioC loading functions."""

    def test_section_config_loading(self):
        """Test section config loading."""
        from bioc_converter.loader import load_section_config

        # Test with None path
        assert load_section_config(None) is None

        # Test with non-existent file
        result = load_section_config("/nonexistent/path.json")
        assert result is None

        # Test with valid config file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(
                {"allowed_sections": ["ABSTRACT", "METHODS"], "case_sensitive": False},
                f,
            )
            config_path = f.name

        try:
            config = load_section_config(config_path)
            assert config is not None
            assert "ABSTRACT" in config["allowed_sections"]
            assert config["case_sensitive"] is False
        finally:
            Path(config_path).unlink()

    def test_is_section_allowed(self):
        """Test section filtering."""
        from bioc_converter.loader import is_section_allowed

        config = {"allowed_sections": ["ABSTRACT", "METHODS"], "case_sensitive": False}

        assert is_section_allowed("ABSTRACT", config) is True
        assert is_section_allowed("abstract", config) is True
        assert is_section_allowed("RESULTS", config) is False
        assert is_section_allowed(None, config) is False

        # With None config, all sections allowed
        assert is_section_allowed("ANY", None) is True

    def test_normalize_section_name(self):
        """Test section name normalization."""
        from bioc_converter.loader import normalize_section_name

        config = {"allowed_sections": ["ABSTRACT"], "case_sensitive": False}

        assert normalize_section_name("ABSTRACT", config) == "ABSTRACT"
        assert normalize_section_name("RESULTS", config) == "OTHER"
        assert normalize_section_name(None, config) == "UNLABELED"
        assert normalize_section_name("ANY", None) == "ANY"

    def test_merge_consecutive_sections(self):
        """Test section merging."""
        from bioc_converter.loader import merge_consecutive_sections

        passages = [
            {"section_type": "ABSTRACT", "passage_index": 0, "start": 0, "end": 100},
            {"section_type": "ABSTRACT", "passage_index": 1, "start": 100, "end": 200},
            {"section_type": "METHODS", "passage_index": 2, "start": 200, "end": 300},
        ]

        merged = merge_consecutive_sections(passages)
        assert len(merged) == 2
        assert merged[0]["section_type"] == "ABSTRACT"
        assert merged[0]["end"] == 200
        assert merged[1]["section_type"] == "METHODS"


class TestSentence:
    """Tests for sentence processing."""

    @pytest.fixture
    def mock_nlp(self):
        """Create a minimal mock NLP object for testing."""
        # Skip if spacy not available
        pytest.importorskip("spacy")
        import spacy

        try:
            nlp = spacy.blank("en")
            nlp.add_pipe("sentencizer")
            return nlp
        except Exception:
            pytest.skip("spaCy not properly configured")

    def test_create_sentence_annotations(self, mock_nlp):
        """Test sentence annotation creation."""
        from bioc_converter.sentence import create_sentence_annotations

        text = "First sentence. Second sentence."
        annotations = [
            {"span": {"begin": 0, "end": 5}, "obj": "WORD"},
            {"span": {"begin": 16, "end": 22}, "obj": "WORD"},
        ]

        result = create_sentence_annotations(text, annotations, mock_nlp)
        assert len(result) == 2
        assert result[0].text == "First sentence."
        assert result[1].text == "Second sentence."


class TestMetadata:
    """Tests for metadata functions."""

    def test_group_sentence_annotations_by_section(self):
        """Test grouping sentences by section."""
        from bioc_converter.metadata import group_sentence_annotations_by_section
        from bioc_converter.models import SentenceAnnotation

        sentences = [
            SentenceAnnotation(text="Title", abs_start=0, abs_end=5),
            SentenceAnnotation(text="Abstract text", abs_start=10, abs_end=23),
            SentenceAnnotation(text="Methods text", abs_start=30, abs_end=42),
        ]

        sections = [
            {"section_type": "TITLE", "start": 0, "end": 10},
            {"section_type": "ABSTRACT", "start": 10, "end": 30},
            {"section_type": "METHODS", "start": 30, "end": 50},
        ]

        grouped = group_sentence_annotations_by_section(sentences, sections)
        assert "TITLE" in grouped
        assert "ABSTRACT" in grouped
        assert "METHODS" in grouped


# Integration test (requires all dependencies)
class TestIntegration:
    """Integration tests requiring full setup."""

    @pytest.mark.skip(reason="Requires clu-bridge and full setup")
    def test_full_conversion(self):
        """Test full conversion pipeline."""
        pass

