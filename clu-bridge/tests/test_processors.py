from clu.bridge import processors
from clu.bridge import odinson
import json
import os
import unittest

# see https://docs.python.org/3/library/unittest.html#basic-example
class TestProcessors(unittest.TestCase):
    doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "processors-doc.json"
    )

    odinson_doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "odinson-doc.json"
    )

    with open(doc_path, "r") as infile:
        odinson_json = json.load(infile)

    def test_load_from(self):
        """processors.Document.parse_file() should load an processors.Document from a path to a Processors Document JSON file."""
        doc = processors.Document.parse_file(TestProcessors.doc_path)
        self.assertTrue(
            isinstance(doc, processors.Document),
            f"{type(doc)} was not a processors.Document",
        )

    def test_create_character_offsets(self):
        """clu.bridge.processors.ConversionUtils.create_character_offsets() should create valid character offsets for each token."""
        tokens = ["I", "like", "turtles", "."]
        text = " ".join(tokens)
        (
            start_offsets,
            end_offsets,
        ) = processors.ConversionUtils.create_character_offsets(tokens)
        for i, tok in enumerate(tokens):
            start, end = start_offsets[i], end_offsets[i]
            span = text[start:end]
            self.assertEqual(tok, span, f"{tok} != {span}")

    def test_to_processors_document(self):
        """clu.bridge.processors.ConversionUtils.to_processors() should convert an Odinson Document to a Processors Document."""
        # pdoc = processors.Document.parse_file(TestConversionUtils.proc_doc_path)
        odinson_doc = odinson.Document.parse_file(TestProcessors.odinson_doc_path)
        res = processors.ConversionUtils.to_processors_document(odinson_doc)
        self.assertTrue(
            isinstance(res, processors.Document),
            f"{type(res)} was not a processors.Document",
        )
        expected = "tp-pies"
        self.assertEqual(res.id, expected, f"{res.id} != '{expected}'")
