# Converting SpaCy Doc and Clu Documents

The `clu-bridge` library can be used to convert SpaCy and [Clu Processors](https://github.com/clulab/processors) documents to and [Odinson](https://github.com/lum-ai/odinson) documents:

# Tutorial

```bash
# download a spacy model
spacy download en_core_web_trf
```

```python
import spacy
from clu.bridge.conversion import ConversionUtils

nlp = spacy.load("en_core_web_trf")

text = "Dale Cooper saw the boy with a telescope."
spacy_doc = nlp(text)

# create a Clu Document
clu_doc = ConversionUtils.spacy.to_clu_document(spacy_doc)

# create an Odinson Document
odinson_doc = ConversionUtils.processors.to_odinson_document(clu_doc)
```