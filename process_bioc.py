import spacy
from bioc_converter import BiocProcessor

# Load NLP model
nlp = spacy.load("en_core_sci_lg")

# Initialize with BioC file - no doc IDs needed!
processor = BiocProcessor("data/10.BioC.XML", nlp)

# Discover what's in the file
print(processor.document_ids)  # ['35215501', '35200688', ...]
print(len(processor))  # Number of documents
print(processor.summary())  # Full collection stats


# Progress callback (optional)
def on_progress(current, total, doc_id, status):
    print(f"[{current}/{total}] {status}: {doc_id}")


# Process and save incrementally - safe from crashes!
# Each document is saved immediately after processing
saved_files = processor.process_and_save(
    output_dir="output/sections/",
    by_sections=True,
    resume=True,  # Skip already-processed docs on re-run
    on_progress=on_progress,
)

# If it crashes, just run again - it will resume from where it left off
print(f"\nSaved {len(saved_files)} files")
