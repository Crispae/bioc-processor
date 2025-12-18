from clu.bridge.conversion import ConversionUtils
from clu.bridge import odinson, processors
import json
import os
import unittest

# see https://docs.python.org/3/library/unittest.html#basic-example
class TestConversionUtils(unittest.TestCase):
    proc_doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "processors-doc.json"
    )
    odinson_doc_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data", "odinson-doc.json"
    )
