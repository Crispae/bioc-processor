"""BioC collection batch processing API.

This module provides a class-based interface for processing entire BioC
collections without specifying individual document IDs.

Recommended Usage (Incremental Save):
    >>> import spacy
    >>> from bioc_converter import BiocProcessor
    >>>
    >>> nlp = spacy.load("en_core_sci_sm")
    >>> processor = BiocProcessor("data/collection.bioc.xml", nlp)
    >>>
    >>> # Process and save incrementally - safe from crashes
    >>> saved_files = processor.process_and_save(
    ...     output_dir="output/sections/",
    ...     by_sections=True,
    ...     resume=True,  # Skip already-processed docs
    ... )
    >>>
    >>> # If it crashes, just run again - it will resume from where it left off

Alternative Usage (In-Memory):
    >>> # Process all documents first, then save
    >>> results = processor.process_all(by_sections=True)
    >>> processor.save_all(results, "output/sections/")
    >>> # Note: Data is lost if process crashes before save_all()
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

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

from .sections import process_bioc_by_sections
from .converter import convert_bioc_to_odinson


@dataclass
class ProcessedDocument:
    """Holds processing results for a single BioC document.

    Attributes:
        doc_id: Document identifier (e.g., PMID)
        sections: Dict mapping section names to Odinson documents (when by_sections=True)
        combined: Single Odinson document containing all sections (when by_sections=False)
        metadata: Additional metadata extracted during processing
    """

    doc_id: str
    sections: Optional[Dict[str, Any]] = None
    combined: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_sectioned(self) -> bool:
        """Check if document was processed by sections."""
        return self.sections is not None

    @property
    def section_names(self) -> List[str]:
        """Get list of section names if processed by sections."""
        if self.sections:
            return list(self.sections.keys())
        return []

    @property
    def total_sentences(self) -> int:
        """Get total number of sentences across all sections/combined doc."""
        total = 0
        if self.sections:
            for odinson_doc in self.sections.values():
                if hasattr(odinson_doc, "sentences"):
                    total += len(odinson_doc.sentences)
        elif self.combined:
            if hasattr(self.combined, "sentences"):
                total = len(self.combined.sentences)
        return total


class BiocProcessor:
    """Class-based interface for processing entire BioC collections.

    This processor loads a BioC file once and provides methods to:
    - Discover all document IDs in the collection
    - Process all documents or selective documents
    - Save results with a flat output structure

    Example:
        >>> processor = BiocProcessor("data/10.BioC.XML", nlp)
        >>> print(processor.document_ids)  # ['35215501', '35200688', ...]
        >>> results = processor.process_all(by_sections=True)
        >>> processor.save_all(results, "output/sections/")
    """

    def __init__(
        self,
        bioc_file_path: str,
        nlp: Language,
        verbose: bool = True,
    ):
        """Initialize processor with BioC file and NLP model.

        Args:
            bioc_file_path: Path to BioC XML file
            nlp: spaCy language model for NLP processing
            verbose: If True, print progress messages

        Raises:
            FileNotFoundError: If BioC file doesn't exist
        """
        self._bioc_path = Path(bioc_file_path)
        if not self._bioc_path.exists():
            raise FileNotFoundError(f"BioC file not found: {self._bioc_path}")

        self._nlp = nlp
        self._verbose = verbose
        self._collection = None
        self._document_ids: Optional[List[str]] = None

        # Load the collection to get document IDs
        self._load_collection()

    def _load_collection(self) -> None:
        """Load the BioC collection from file."""
        if self._verbose:
            print(f"Loading BioC collection: {self._bioc_path}")

        with open(self._bioc_path, "r", encoding="utf-8") as f:
            self._collection = bioc_load(f)

        self._document_ids = [doc.id for doc in self._collection.documents]

        if self._verbose:
            print(f"Found {len(self._document_ids)} documents in collection")

    @property
    def document_ids(self) -> List[str]:
        """List all document IDs in the collection.

        Returns:
            List of document ID strings
        """
        return self._document_ids.copy() if self._document_ids else []

    @property
    def bioc_path(self) -> Path:
        """Get the path to the BioC file."""
        return self._bioc_path

    @property
    def collection_size(self) -> int:
        """Get the number of documents in the collection."""
        return len(self._document_ids) if self._document_ids else 0

    def __len__(self) -> int:
        """Return the number of documents in the collection."""
        return self.collection_size

    def __contains__(self, document_id: str) -> bool:
        """Check if a document ID exists in the collection."""
        return document_id in self._document_ids if self._document_ids else False

    def process(
        self,
        document_id: str,
        by_sections: bool = True,
    ) -> ProcessedDocument:
        """Process a single document from the collection.

        Args:
            document_id: ID of the document to process
            by_sections: If True, create separate Odinson docs per section.
                        If False, create one combined Odinson document.

        Returns:
            ProcessedDocument containing the conversion results

        Raises:
            ValueError: If document_id not found in collection
        """
        if document_id not in self:
            raise ValueError(
                f"Document ID '{document_id}' not found in collection. "
                f"Available IDs: {self._document_ids[:5]}..."
            )

        if self._verbose:
            print(f"\nProcessing document: {document_id}")

        if by_sections:
            # Use section-based processing
            sections = process_bioc_by_sections(
                bioc_file_path=str(self._bioc_path),
                document_id=document_id,
                nlp=self._nlp,
                verbose=self._verbose,
            )
            return ProcessedDocument(
                doc_id=document_id,
                sections=sections,
                combined=None,
            )
        else:
            # Use combined processing
            combined = convert_bioc_to_odinson(
                bioc_file_path=str(self._bioc_path),
                document_id=document_id,
                nlp=self._nlp,
                combine_sentences=True,
                verbose=self._verbose,
            )
            return ProcessedDocument(
                doc_id=document_id,
                sections=None,
                combined=combined,
            )

    def process_all(
        self,
        by_sections: bool = True,
        on_progress: Optional[Callable[[int, int, str], None]] = None,
        document_ids: Optional[List[str]] = None,
    ) -> Dict[str, ProcessedDocument]:
        """Process all documents (or a subset) in the collection.

        Args:
            by_sections: If True, create separate Odinson docs per section.
                        If False, create one combined Odinson document per doc.
            on_progress: Optional callback function called after each document.
                        Receives (current_index, total_count, document_id).
            document_ids: Optional list of specific document IDs to process.
                         If None, processes all documents in the collection.

        Returns:
            Dictionary mapping document IDs to ProcessedDocument objects

        Example:
            >>> def progress_callback(current, total, doc_id):
            ...     print(f"Processed {current}/{total}: {doc_id}")
            >>> results = processor.process_all(on_progress=progress_callback)
        """
        ids_to_process = document_ids if document_ids else self._document_ids
        if not ids_to_process:
            return {}

        total = len(ids_to_process)
        results: Dict[str, ProcessedDocument] = {}

        if self._verbose:
            print(f"\nProcessing {total} documents from collection...")

        for idx, doc_id in enumerate(ids_to_process, start=1):
            try:
                result = self.process(doc_id, by_sections=by_sections)
                results[doc_id] = result

                if on_progress:
                    on_progress(idx, total, doc_id)
                elif self._verbose:
                    print(f"  [{idx}/{total}] Completed: {doc_id}")

            except Exception as e:
                if self._verbose:
                    print(f"  [{idx}/{total}] Failed: {doc_id} - {e}")
                # Store error in metadata
                results[doc_id] = ProcessedDocument(
                    doc_id=doc_id,
                    metadata={"error": str(e)},
                )

        if self._verbose:
            successful = sum(1 for r in results.values() if r.sections or r.combined)
            print(f"\nProcessed {successful}/{total} documents successfully")

        return results

    def save_all(
        self,
        results: Dict[str, ProcessedDocument],
        output_dir: str,
        compress: bool = False,
        verbose: Optional[bool] = None,
    ) -> List[Path]:
        """Save all processed results to output directory (flat structure).

        Files are saved with naming pattern: {doc_id}_{section_name}.json

        Args:
            results: Dictionary of ProcessedDocument objects from process_all()
            output_dir: Path to output directory
            compress: If True, also create a tar.gz archive of all files
            verbose: Override instance verbose setting for this operation

        Returns:
            List of saved file paths

        Example:
            >>> results = processor.process_all()
            >>> saved_files = processor.save_all(results, "output/sections/")
            >>> # Creates: output/sections/35215501_title.json,
            >>> #          output/sections/35215501_abstract.json, etc.
        """
        if verbose is None:
            verbose = self._verbose

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        saved_files: List[Path] = []

        if verbose:
            print(f"\nSaving results to: {output_path}")

        for doc_id, processed_doc in results.items():
            # Skip documents with errors
            if processed_doc.metadata.get("error"):
                if verbose:
                    print(f"  Skipping {doc_id} (processing error)")
                continue

            if processed_doc.is_sectioned and processed_doc.sections:
                # Save each section as separate file
                for section_name, odinson_doc in processed_doc.sections.items():
                    file_path = self._save_section_file(
                        odinson_doc=odinson_doc,
                        doc_id=doc_id,
                        section_name=section_name,
                        output_path=output_path,
                        verbose=verbose,
                    )
                    if file_path:
                        saved_files.append(file_path)

            elif processed_doc.combined:
                # Save combined document
                file_path = self._save_section_file(
                    odinson_doc=processed_doc.combined,
                    doc_id=doc_id,
                    section_name="combined",
                    output_path=output_path,
                    verbose=verbose,
                )
                if file_path:
                    saved_files.append(file_path)

        # Create compressed archive if requested
        if compress and saved_files:
            archive_path = self._create_archive(saved_files, output_path, verbose)
            if archive_path:
                saved_files.append(archive_path)

        if verbose:
            print(f"\nSaved {len(saved_files)} files to: {output_path}")

        return saved_files

    def process_and_save(
        self,
        output_dir: str,
        by_sections: bool = True,
        resume: bool = True,
        compress: bool = False,
        on_progress: Optional[Callable[[int, int, str, str], None]] = None,
        document_ids: Optional[List[str]] = None,
    ) -> List[Path]:
        """Process and save each document incrementally.

        Unlike process_all() + save_all(), this method saves each document
        immediately after processing. This prevents data loss if the process
        crashes or is interrupted.

        Args:
            output_dir: Directory to save output files
            by_sections: If True, create separate files per section.
                        If False, create one combined file per document.
            resume: If True, skip documents that already have output files
                   in the output directory. Useful for resuming interrupted runs.
            compress: If True, create a tar.gz archive after all processing
            on_progress: Optional callback function called after each document.
                        Receives (current_index, total_count, document_id, status).
                        Status is 'processed', 'skipped', or 'failed'.
            document_ids: Optional list of specific document IDs to process.
                         If None, processes all documents in the collection.

        Returns:
            List of saved file paths

        Example:
            >>> processor = BiocProcessor("data/10.BioC.XML", nlp)
            >>>
            >>> # Process and save incrementally - safe from crashes
            >>> saved = processor.process_and_save(
            ...     output_dir="output/sections/",
            ...     by_sections=True,
            ...     resume=True,
            ... )
            >>>
            >>> # If it crashes, just run again - it will resume
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Determine which documents to process
        ids_to_process = document_ids if document_ids else self._document_ids
        if not ids_to_process:
            return []

        # Check for existing files if resume is enabled
        skipped_ids: Set[str] = set()
        if resume:
            existing_ids = self._get_existing_doc_ids(output_path)
            skipped_ids = set(ids_to_process) & existing_ids
            ids_to_process = [
                doc_id for doc_id in ids_to_process if doc_id not in existing_ids
            ]

            if self._verbose and skipped_ids:
                print(
                    f"Resume mode: Skipping {len(skipped_ids)} already-processed "
                    f"documents"
                )

        total_original = len(document_ids) if document_ids else len(self._document_ids)
        total_to_process = len(ids_to_process)

        if self._verbose:
            print(f"\nProcessing {total_to_process} documents...")
            if skipped_ids:
                print(f"  (Skipped {len(skipped_ids)} existing)")

        saved_files: List[Path] = []
        processed_count = 0
        failed_count = 0

        for idx, doc_id in enumerate(ids_to_process, start=1):
            try:
                # Process the document
                result = self.process(doc_id, by_sections=by_sections)

                # Save immediately after processing
                if result.is_sectioned and result.sections:
                    for section_name, odinson_doc in result.sections.items():
                        file_path = self._save_section_file(
                            odinson_doc=odinson_doc,
                            doc_id=doc_id,
                            section_name=section_name,
                            output_path=output_path,
                            verbose=False,  # Suppress individual file messages
                        )
                        if file_path:
                            saved_files.append(file_path)

                elif result.combined:
                    file_path = self._save_section_file(
                        odinson_doc=result.combined,
                        doc_id=doc_id,
                        section_name="combined",
                        output_path=output_path,
                        verbose=False,
                    )
                    if file_path:
                        saved_files.append(file_path)

                processed_count += 1
                status = "processed"

                if on_progress:
                    on_progress(idx, total_to_process, doc_id, status)
                elif self._verbose:
                    print(
                        f"  [{idx}/{total_to_process}] Saved: {doc_id} "
                        f"({result.total_sentences} sentences)"
                    )

            except Exception as e:
                failed_count += 1
                status = "failed"

                if on_progress:
                    on_progress(idx, total_to_process, doc_id, status)
                elif self._verbose:
                    print(f"  [{idx}/{total_to_process}] Failed: {doc_id} - {e}")

        # Create compressed archive if requested
        if compress and saved_files:
            archive_path = self._create_archive(saved_files, output_path, self._verbose)
            if archive_path:
                saved_files.append(archive_path)

        if self._verbose:
            print(f"\nCompleted:")
            print(f"  Processed: {processed_count}")
            print(f"  Skipped (existing): {len(skipped_ids)}")
            print(f"  Failed: {failed_count}")
            print(f"  Total files saved: {len(saved_files)}")

        return saved_files

    def _get_existing_doc_ids(self, output_dir: Path) -> Set[str]:
        """Find document IDs that already have output files.

        Scans the output directory for JSON files matching the naming pattern
        {doc_id}_{section}.json and extracts unique document IDs.

        Args:
            output_dir: Directory to scan for existing files

        Returns:
            Set of document IDs that have existing output files
        """
        existing_ids: Set[str] = set()

        if not output_dir.exists():
            return existing_ids

        # Scan for JSON files matching our naming pattern
        for json_file in output_dir.glob("*.json"):
            filename = json_file.stem  # e.g., "35215501_title"

            # Extract doc_id (everything before the last underscore)
            # Handle cases like "35215501_intro" or "35215501_results_discussion"
            parts = filename.rsplit("_", 1)
            if len(parts) >= 1:
                potential_doc_id = parts[0]

                # Verify this doc_id exists in our collection
                # Check both the raw ID and sanitized version
                for doc_id in self._document_ids:
                    sanitized = self._sanitize_filename(doc_id)
                    if potential_doc_id == sanitized or potential_doc_id == doc_id:
                        existing_ids.add(doc_id)
                        break

        return existing_ids

    def _save_section_file(
        self,
        odinson_doc: Any,
        doc_id: str,
        section_name: str,
        output_path: Path,
        verbose: bool,
    ) -> Optional[Path]:
        """Save a single Odinson document to a JSON file.

        Args:
            odinson_doc: Odinson Document object
            doc_id: Document ID for filename
            section_name: Section name for filename
            output_path: Output directory path
            verbose: Whether to print progress

        Returns:
            Path to saved file, or None if save failed
        """
        # Sanitize names for filename
        doc_id_safe = self._sanitize_filename(doc_id)
        section_safe = self._sanitize_filename(section_name)

        filename = f"{doc_id_safe}_{section_safe}.json"
        file_path = output_path / filename

        try:
            # Serialize to JSON (handle both pydantic v1 and v2)
            try:
                json_output = odinson_doc.model_dump_json(indent=2, by_alias=True)
            except AttributeError:
                json_output = odinson_doc.json(by_alias=True, indent=2)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_output)

            if verbose:
                num_sentences = (
                    len(odinson_doc.sentences)
                    if hasattr(odinson_doc, "sentences")
                    else 0
                )
                file_size = file_path.stat().st_size / 1024
                print(
                    f"  Saved: {filename} "
                    f"({num_sentences} sentences, {file_size:.1f} KB)"
                )

            return file_path

        except Exception as e:
            if verbose:
                print(f"  Error saving {filename}: {e}")
            return None

    def _create_archive(
        self,
        files: List[Path],
        output_path: Path,
        verbose: bool,
    ) -> Optional[Path]:
        """Create a tar.gz archive of all saved files.

        Args:
            files: List of file paths to include
            output_path: Output directory for archive
            verbose: Whether to print progress

        Returns:
            Path to archive file, or None if creation failed
        """
        import tarfile

        archive_name = "bioc_collection.tar.gz"
        archive_path = output_path / archive_name

        try:
            with tarfile.open(archive_path, "w:gz") as tar:
                for file_path in files:
                    if file_path.suffix == ".json":  # Only include JSON files
                        tar.add(file_path, arcname=file_path.name)

            if verbose:
                archive_size = archive_path.stat().st_size / 1024
                print(f"\nCreated archive: {archive_name} ({archive_size:.1f} KB)")

            return archive_path

        except Exception as e:
            if verbose:
                print(f"  Error creating archive: {e}")
            return None

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """Sanitize a string for use in a filename.

        Args:
            name: String to sanitize

        Returns:
            Sanitized string safe for filenames
        """
        # Convert to lowercase and replace unsafe characters
        safe_name = re.sub(r"[^a-z0-9_-]+", "_", name.lower())
        # Remove leading/trailing underscores
        safe_name = safe_name.strip("_")
        return safe_name or "unknown"

    def get_document_info(self, document_id: str) -> Dict[str, Any]:
        """Get metadata information about a specific document.

        Args:
            document_id: ID of the document

        Returns:
            Dictionary with document metadata (passages count, infons, etc.)

        Raises:
            ValueError: If document_id not found
        """
        if document_id not in self:
            raise ValueError(f"Document ID '{document_id}' not found in collection")

        for doc in self._collection.documents:
            if doc.id == document_id:
                return {
                    "id": doc.id,
                    "passages_count": len(doc.passages),
                    "infons": dict(doc.infons) if doc.infons else {},
                    "annotations_count": sum(len(p.annotations) for p in doc.passages),
                }

        return {}

    def summary(self) -> Dict[str, Any]:
        """Get a summary of the entire collection.

        Returns:
            Dictionary with collection statistics
        """
        total_passages = 0
        total_annotations = 0

        for doc in self._collection.documents:
            total_passages += len(doc.passages)
            total_annotations += sum(len(p.annotations) for p in doc.passages)

        return {
            "file_path": str(self._bioc_path),
            "document_count": len(self._document_ids),
            "document_ids": self._document_ids,
            "total_passages": total_passages,
            "total_annotations": total_annotations,
        }


