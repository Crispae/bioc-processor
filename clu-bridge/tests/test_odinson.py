from clu.bridge import odinson
from clu.bridge import processors
import json
import os
import unittest

# see https://docs.python.org/3/library/unittest.html#basic-example
class TestOdinson(unittest.TestCase):

    odinson_doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "odinson-doc.json"
    )

    proc_doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "processors-doc.json"
    )

    with open(odinson_doc_path, "r") as infile:
        odinson_json = json.load(infile)

    def test_load_from(self):
        """odinson.Document.parse_file() should load an odinson.Document from a path to a Odinson Document JSON file."""
        od = odinson.Document.parse_file(TestOdinson.odinson_doc_path)
        self.assertTrue(
            isinstance(od, odinson.Document), f"{type(od)} was not an odinson.Document"
        )

    def test_to_odinson_document(self):
        """clu.bridge.odinson.ConversionUtils.to_processors() should convert a Processors Document to an Odinson Document."""
        clu_doc = processors.Document.parse_file(TestOdinson.proc_doc_path)
        res = processors.ConversionUtils.to_odinson_document(clu_doc)
        self.assertTrue(
            isinstance(res, odinson.Document),
            f"{type(res)} was not an odinson.Document",
        )
        expected = "tp-pies"
        self.assertIsInstance(
            res, odinson.Document, f"{res} is not '{odinson.Document}'"
        )
