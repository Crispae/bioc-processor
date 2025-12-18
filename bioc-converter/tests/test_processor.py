"""Tests for BiocProcessor batch processing API."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestProcessedDocument:
    """Tests for ProcessedDocument dataclass."""

    def test_processed_document_creation(self):
        """Test ProcessedDocument dataclass creation."""
        from bioc_converter.processor import ProcessedDocument

        doc = ProcessedDocument(doc_id="12345")
        assert doc.doc_id == "12345"
        assert doc.sections is None
        assert doc.combined is None
        assert doc.metadata == {}

    def test_processed_document_with_sections(self):
        """Test ProcessedDocument with sections data."""
        from bioc_converter.processor import ProcessedDocument

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock(), MagicMock(), MagicMock()]

        doc = ProcessedDocument(
            doc_id="12345",
            sections={"TITLE": mock_odinson_doc, "ABSTRACT": mock_odinson_doc},
        )

        assert doc.is_sectioned is True
        assert doc.section_names == ["TITLE", "ABSTRACT"]
        assert doc.total_sentences == 6  # 3 sentences * 2 sections

    def test_processed_document_with_combined(self):
        """Test ProcessedDocument with combined data."""
        from bioc_converter.processor import ProcessedDocument

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()] * 5

        doc = ProcessedDocument(
            doc_id="12345",
            combined=mock_odinson_doc,
        )

        assert doc.is_sectioned is False
        assert doc.section_names == []
        assert doc.total_sentences == 5

    def test_processed_document_with_error(self):
        """Test ProcessedDocument with error metadata."""
        from bioc_converter.processor import ProcessedDocument

        doc = ProcessedDocument(
            doc_id="12345",
            metadata={"error": "Failed to process"},
        )

        assert doc.is_sectioned is False
        assert doc.total_sentences == 0
        assert doc.metadata.get("error") == "Failed to process"


class TestBiocProcessorInit:
    """Tests for BiocProcessor initialization."""

    @pytest.fixture
    def sample_bioc_file(self, tmp_path):
        """Create a sample BioC XML file for testing."""
        bioc_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE collection SYSTEM "BioC.dtd">
<collection>
  <source>Test</source>
  <date>2024-01-01</date>
  <key>test.key</key>
  <document>
    <id>12345</id>
    <infon key="article-id_pmid">12345</infon>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Test Title</text>
    </passage>
    <passage>
      <infon key="section_type">ABSTRACT</infon>
      <offset>11</offset>
      <text>Test abstract content here.</text>
    </passage>
  </document>
  <document>
    <id>67890</id>
    <infon key="article-id_pmid">67890</infon>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Another Title</text>
    </passage>
  </document>
</collection>"""
        bioc_file = tmp_path / "test.bioc.xml"
        bioc_file.write_text(bioc_content, encoding="utf-8")
        return bioc_file

    @pytest.fixture
    def mock_nlp(self):
        """Create a mock NLP object."""
        nlp = MagicMock()
        nlp.pipe_names = []
        return nlp

    def test_processor_initialization(self, sample_bioc_file, mock_nlp):
        """Test BiocProcessor initialization."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        assert processor.collection_size == 2
        assert len(processor) == 2
        assert processor.document_ids == ["12345", "67890"]
        assert processor.bioc_path == sample_bioc_file

    def test_processor_file_not_found(self, mock_nlp):
        """Test BiocProcessor raises error for missing file."""
        from bioc_converter.processor import BiocProcessor

        with pytest.raises(FileNotFoundError):
            BiocProcessor("/nonexistent/file.xml", mock_nlp)

    def test_processor_contains(self, sample_bioc_file, mock_nlp):
        """Test BiocProcessor __contains__ method."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        assert "12345" in processor
        assert "67890" in processor
        assert "99999" not in processor

    def test_processor_summary(self, sample_bioc_file, mock_nlp):
        """Test BiocProcessor summary method."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        summary = processor.summary()

        assert summary["document_count"] == 2
        assert summary["document_ids"] == ["12345", "67890"]
        assert summary["total_passages"] == 3
        assert str(sample_bioc_file) in summary["file_path"]

    def test_processor_get_document_info(self, sample_bioc_file, mock_nlp):
        """Test BiocProcessor get_document_info method."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        info = processor.get_document_info("12345")

        assert info["id"] == "12345"
        assert info["passages_count"] == 2
        assert "article-id_pmid" in info["infons"]

    def test_processor_get_document_info_not_found(self, sample_bioc_file, mock_nlp):
        """Test get_document_info raises error for missing document."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        with pytest.raises(ValueError, match="not found"):
            processor.get_document_info("99999")


class TestBiocProcessorProcess:
    """Tests for BiocProcessor process methods."""

    @pytest.fixture
    def sample_bioc_file(self, tmp_path):
        """Create a sample BioC XML file for testing."""
        bioc_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE collection SYSTEM "BioC.dtd">
<collection>
  <source>Test</source>
  <date>2024-01-01</date>
  <key>test.key</key>
  <document>
    <id>12345</id>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Test Title</text>
    </passage>
  </document>
  <document>
    <id>67890</id>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Another Title</text>
    </passage>
  </document>
</collection>"""
        bioc_file = tmp_path / "test.bioc.xml"
        bioc_file.write_text(bioc_content, encoding="utf-8")
        return bioc_file

    @pytest.fixture
    def mock_nlp(self):
        """Create a mock NLP object."""
        nlp = MagicMock()
        nlp.pipe_names = []
        return nlp

    def test_process_invalid_document_id(self, sample_bioc_file, mock_nlp):
        """Test process raises error for invalid document ID."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        with pytest.raises(ValueError, match="not found"):
            processor.process("99999")

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_by_sections(
        self, mock_process_sections, sample_bioc_file, mock_nlp
    ):
        """Test process method with by_sections=True."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        result = processor.process("12345", by_sections=True)

        assert result.doc_id == "12345"
        assert result.is_sectioned is True
        assert "TITLE" in result.sections
        mock_process_sections.assert_called_once()

    @patch("bioc_converter.processor.convert_bioc_to_odinson")
    def test_process_combined(self, mock_convert, sample_bioc_file, mock_nlp):
        """Test process method with by_sections=False."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_convert.return_value = mock_odinson_doc

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        result = processor.process("12345", by_sections=False)

        assert result.doc_id == "12345"
        assert result.is_sectioned is False
        assert result.combined == mock_odinson_doc
        mock_convert.assert_called_once()

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_all(self, mock_process_sections, sample_bioc_file, mock_nlp):
        """Test process_all method."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        results = processor.process_all(by_sections=True)

        assert len(results) == 2
        assert "12345" in results
        assert "67890" in results
        assert results["12345"].is_sectioned is True

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_all_with_callback(
        self, mock_process_sections, sample_bioc_file, mock_nlp
    ):
        """Test process_all with progress callback."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        callback_calls = []

        def progress_callback(current, total, doc_id):
            callback_calls.append((current, total, doc_id))

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        results = processor.process_all(by_sections=True, on_progress=progress_callback)

        assert len(callback_calls) == 2
        assert callback_calls[0] == (1, 2, "12345")
        assert callback_calls[1] == (2, 2, "67890")

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_all_subset(
        self, mock_process_sections, sample_bioc_file, mock_nlp
    ):
        """Test process_all with specific document IDs."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        results = processor.process_all(by_sections=True, document_ids=["12345"])

        assert len(results) == 1
        assert "12345" in results
        assert "67890" not in results


class TestBiocProcessorSave:
    """Tests for BiocProcessor save methods."""

    @pytest.fixture
    def sample_bioc_file(self, tmp_path):
        """Create a sample BioC XML file for testing."""
        bioc_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE collection SYSTEM "BioC.dtd">
<collection>
  <source>Test</source>
  <date>2024-01-01</date>
  <key>test.key</key>
  <document>
    <id>12345</id>
    <passage>
      <offset>0</offset>
      <text>Test</text>
    </passage>
  </document>
</collection>"""
        bioc_file = tmp_path / "test.bioc.xml"
        bioc_file.write_text(bioc_content, encoding="utf-8")
        return bioc_file

    @pytest.fixture
    def mock_nlp(self):
        """Create a mock NLP object."""
        nlp = MagicMock()
        nlp.pipe_names = []
        return nlp

    def test_save_all_sectioned(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test save_all with sectioned results."""
        from bioc_converter.processor import BiocProcessor, ProcessedDocument

        # Create mock Odinson documents
        mock_title_doc = MagicMock()
        mock_title_doc.sentences = [MagicMock()]
        mock_title_doc.model_dump_json.return_value = '{"id": "test_title"}'

        mock_abstract_doc = MagicMock()
        mock_abstract_doc.sentences = [MagicMock(), MagicMock()]
        mock_abstract_doc.model_dump_json.return_value = '{"id": "test_abstract"}'

        results = {
            "12345": ProcessedDocument(
                doc_id="12345",
                sections={"TITLE": mock_title_doc, "ABSTRACT": mock_abstract_doc},
            )
        }

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        saved_files = processor.save_all(results, str(output_dir), verbose=False)

        assert len(saved_files) == 2
        assert (output_dir / "12345_title.json").exists()
        assert (output_dir / "12345_abstract.json").exists()

    def test_save_all_combined(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test save_all with combined results."""
        from bioc_converter.processor import BiocProcessor, ProcessedDocument

        mock_combined_doc = MagicMock()
        mock_combined_doc.sentences = [MagicMock()]
        mock_combined_doc.model_dump_json.return_value = '{"id": "test_combined"}'

        results = {
            "12345": ProcessedDocument(
                doc_id="12345",
                combined=mock_combined_doc,
            )
        }

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        saved_files = processor.save_all(results, str(output_dir), verbose=False)

        assert len(saved_files) == 1
        assert (output_dir / "12345_combined.json").exists()

    def test_save_all_skips_errors(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test save_all skips documents with errors."""
        from bioc_converter.processor import BiocProcessor, ProcessedDocument

        results = {
            "12345": ProcessedDocument(
                doc_id="12345",
                metadata={"error": "Processing failed"},
            )
        }

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        saved_files = processor.save_all(results, str(output_dir), verbose=False)

        assert len(saved_files) == 0

    def test_save_all_with_compression(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test save_all with compression enabled."""
        from bioc_converter.processor import BiocProcessor, ProcessedDocument

        mock_doc = MagicMock()
        mock_doc.sentences = [MagicMock()]
        mock_doc.model_dump_json.return_value = '{"id": "test"}'

        results = {
            "12345": ProcessedDocument(
                doc_id="12345",
                sections={"TITLE": mock_doc},
            )
        }

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        saved_files = processor.save_all(
            results, str(output_dir), compress=True, verbose=False
        )

        # Should have JSON file + archive
        assert len(saved_files) == 2
        archive_file = [f for f in saved_files if f.suffix == ".gz"]
        assert len(archive_file) == 1

    def test_sanitize_filename(self, sample_bioc_file, mock_nlp):
        """Test filename sanitization."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        assert processor._sanitize_filename("ABSTRACT") == "abstract"
        assert (
            processor._sanitize_filename("Results & Discussion") == "results_discussion"
        )
        assert processor._sanitize_filename("Test/Name") == "test_name"
        assert processor._sanitize_filename("") == "unknown"
        assert processor._sanitize_filename("___") == "unknown"


class TestBiocProcessorIncrementalSave:
    """Tests for process_and_save incremental saving functionality."""

    @pytest.fixture
    def sample_bioc_file(self, tmp_path):
        """Create a sample BioC XML file for testing."""
        bioc_content = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE collection SYSTEM "BioC.dtd">
<collection>
  <source>Test</source>
  <date>2024-01-01</date>
  <key>test.key</key>
  <document>
    <id>12345</id>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Test Title</text>
    </passage>
  </document>
  <document>
    <id>67890</id>
    <passage>
      <infon key="section_type">TITLE</infon>
      <offset>0</offset>
      <text>Another Title</text>
    </passage>
  </document>
  <document>
    <id>11111</id>
    <passage>
      <infon key="section_type">ABSTRACT</infon>
      <offset>0</offset>
      <text>Third document abstract.</text>
    </passage>
  </document>
</collection>"""
        bioc_file = tmp_path / "test.bioc.xml"
        bioc_file.write_text(bioc_content, encoding="utf-8")
        return bioc_file

    @pytest.fixture
    def mock_nlp(self):
        """Create a mock NLP object."""
        nlp = MagicMock()
        nlp.pipe_names = []
        return nlp

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_basic(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test basic process_and_save functionality."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "test"}'
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"

        saved_files = processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            resume=False,
        )

        # Should have 3 files (one per document)
        assert len(saved_files) == 3
        assert (output_dir / "12345_title.json").exists()
        assert (output_dir / "67890_title.json").exists()
        assert (output_dir / "11111_title.json").exists()

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_with_callback(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test process_and_save with progress callback."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "test"}'
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        callback_calls = []

        def progress_callback(current, total, doc_id, status):
            callback_calls.append((current, total, doc_id, status))

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"

        processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            resume=False,
            on_progress=progress_callback,
        )

        assert len(callback_calls) == 3
        assert callback_calls[0] == (1, 3, "12345", "processed")
        assert callback_calls[1] == (2, 3, "67890", "processed")
        assert callback_calls[2] == (3, 3, "11111", "processed")

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_resume_skips_existing(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test that resume=True skips already-processed documents."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "test"}'
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        # Create output directory with existing file for doc 12345
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        existing_file = output_dir / "12345_title.json"
        existing_file.write_text('{"existing": true}')

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        saved_files = processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            resume=True,  # Should skip 12345
        )

        # Should only process 2 documents (67890 and 11111), skipping 12345
        assert len(saved_files) == 2
        assert (output_dir / "67890_title.json").exists()
        assert (output_dir / "11111_title.json").exists()

        # Verify process_bioc_by_sections was only called twice (not for 12345)
        assert mock_process_sections.call_count == 2

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_resume_false_overwrites(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test that resume=False processes all documents even if files exist."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "new"}'
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        # Create output directory with existing file
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        existing_file = output_dir / "12345_title.json"
        existing_file.write_text('{"existing": true}')

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)

        saved_files = processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            resume=False,  # Should process all, overwriting existing
        )

        # Should process all 3 documents
        assert len(saved_files) == 3
        assert mock_process_sections.call_count == 3

        # Verify existing file was overwritten
        content = existing_file.read_text()
        assert "new" in content

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_subset(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test process_and_save with specific document IDs."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "test"}'
        mock_process_sections.return_value = {"TITLE": mock_odinson_doc}

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"

        saved_files = processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            document_ids=["12345", "67890"],  # Only process these two
        )

        assert len(saved_files) == 2
        assert (output_dir / "12345_title.json").exists()
        assert (output_dir / "67890_title.json").exists()
        assert not (output_dir / "11111_title.json").exists()

    def test_get_existing_doc_ids_empty_dir(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test _get_existing_doc_ids with empty directory."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        existing = processor._get_existing_doc_ids(output_dir)
        assert existing == set()

    def test_get_existing_doc_ids_nonexistent_dir(
        self, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test _get_existing_doc_ids with non-existent directory."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "nonexistent"

        existing = processor._get_existing_doc_ids(output_dir)
        assert existing == set()

    def test_get_existing_doc_ids_with_files(self, sample_bioc_file, mock_nlp, tmp_path):
        """Test _get_existing_doc_ids correctly identifies existing documents."""
        from bioc_converter.processor import BiocProcessor

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create some existing files
        (output_dir / "12345_title.json").write_text("{}")
        (output_dir / "12345_abstract.json").write_text("{}")
        (output_dir / "67890_intro.json").write_text("{}")
        (output_dir / "unrelated_file.json").write_text("{}")  # Should be ignored

        existing = processor._get_existing_doc_ids(output_dir)

        assert "12345" in existing
        assert "67890" in existing
        assert "11111" not in existing  # No file for this doc
        assert len(existing) == 2

    @patch("bioc_converter.processor.process_bioc_by_sections")
    def test_process_and_save_handles_failures(
        self, mock_process_sections, sample_bioc_file, mock_nlp, tmp_path
    ):
        """Test that process_and_save continues after a document fails."""
        from bioc_converter.processor import BiocProcessor

        mock_odinson_doc = MagicMock()
        mock_odinson_doc.sentences = [MagicMock()]
        mock_odinson_doc.model_dump_json.return_value = '{"id": "test"}'

        # First call fails, second and third succeed
        mock_process_sections.side_effect = [
            Exception("Processing failed"),
            {"TITLE": mock_odinson_doc},
            {"ABSTRACT": mock_odinson_doc},
        ]

        callback_calls = []

        def progress_callback(current, total, doc_id, status):
            callback_calls.append((doc_id, status))

        processor = BiocProcessor(str(sample_bioc_file), mock_nlp, verbose=False)
        output_dir = tmp_path / "output"

        saved_files = processor.process_and_save(
            output_dir=str(output_dir),
            by_sections=True,
            resume=False,
            on_progress=progress_callback,
        )

        # Should have 2 files (67890 and 11111 succeeded)
        assert len(saved_files) == 2

        # Verify callback received correct statuses
        assert ("12345", "failed") in callback_calls
        assert ("67890", "processed") in callback_calls
        assert ("11111", "processed") in callback_calls


class TestBiocProcessorIntegration:
    """Integration tests requiring full dependencies."""

    @pytest.mark.skip(reason="Requires clu-bridge and full NLP setup")
    def test_full_processing_pipeline(self):
        """Test complete processing pipeline."""
        pass

