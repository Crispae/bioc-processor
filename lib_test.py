import spacy
from bioc_converter import process_bioc_by_sections, save_odinson_sections

# Load model
nlp = spacy.load("en_core_sci_lg")  # or en_core_web_sm

# Convert
odinson_docs = process_bioc_by_sections(
    bioc_file_path="data/10.BioC.XML",
    document_id="35215501",  # Use actual doc ID from your file
    nlp=nlp,
)

# Save
# Save each section as separate JSON
save_odinson_sections(
    odinson_docs=odinson_docs,
    folder_path="output/sections/",
    doc_name="35200688",
    compress=True,
)
