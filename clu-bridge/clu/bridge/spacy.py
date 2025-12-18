from typing import Iterable, Union, Dict
from spacy.tokens import Token, Doc, Span as SpacySpan
from spacy.language import Language
from clu.bridge import processors


class ConversionUtils:
    @staticmethod
    def _peek(generator: Iterable) -> Union[Token, None]:
        """peek() will return either the next Spacy Token in the iterable or None."""
        try:
            first = next(generator)
            return first
        except StopIteration:
            return None

    @staticmethod
    def _spaces_to_offsets(sent: SpacySpan) -> tuple[list[int], list[int]]:
        """
        Converts a SpaCy Span to start and end offsets.

        Parameters
        ----------
        sent: a SpaCy Span object

        Returns
        -------
        start_offsets: list of start offsets
        end_offsets: list of end offsets
        """
        start_offsets = []
        end_offsets = []
        for token in sent:
            start_offsets.append(token.idx)
            end_offsets.append(token.idx + len(token))
        return start_offsets, end_offsets

    @staticmethod
    def to_clu_graph(
        sent: SpacySpan,
    ) -> Dict[processors.Graphs, processors.DirectedGraph]:
        """
        Converts a SpaCy Span to a CLU Processors DirectedGraph.

        Parameters
        ----------
        sent: a SpaCy Span object

        Returns
        -------
        graph: a processors DirectedGraph object
        """
        edges = []
        for token in sent:
            if token.dep_ != "ROOT":
                head_idx = token.head.i - sent.start
                token_idx = token.i - sent.start
                edges.append(
                    processors.Edge(
                        source=head_idx,
                        destination=token_idx,
                        relation=token.dep_,
                    )
                )
        return {
            processors.Graphs.UNIVERSAL_BASIC: processors.DirectedGraph(
                edges=edges,
                roots=[token.i - sent.start for token in sent if token.dep_ == "ROOT"]
            )
        }

    @staticmethod
    def to_clu_sentence(sent: SpacySpan) -> processors.Sentence:
        """
        Converts a SpaCy Span (Doc slice) object to a processors Sentence object.

        Parameters
        ----------
        sent: a SpaCy Span object

        Returns
        -------
        sentence: a processors Sentence object
        """

        start_offsets, end_offsets = ConversionUtils._spaces_to_offsets(sent)

        # Extract entities from BioC annotations (added via CustomNer component)
        # Create entity labels in IOB format: B-LABEL, I-LABEL, or O
        # We use ONLY BioC annotations, NOT spaCy's NER
        entities = []
        for token in sent:
            entity_label = "O"
            
            # Check all entity spans in the document (added by CustomNer from BioC)
            for ent in sent.doc.ents:
                # Check if this entity overlaps with the sentence span
                if (ent.start_char < sent.end_char and ent.end_char > sent.start_char):
                    # Check if this token is part of this entity
                    if token.idx >= ent.start_char and (token.idx + len(token)) <= ent.end_char:
                        # Determine if this is the beginning (B) or inside (I) of the entity
                        if token.idx == ent.start_char:
                            entity_label = f"B-{ent.label_}"
                        else:
                            entity_label = f"I-{ent.label_}"
                        break
            
            entities.append(entity_label)

        sentence = processors.Sentence(
            raw=[token.text for token in sent],
            startOffsets=start_offsets,
            endOffsets=end_offsets,
            words=[token.text for token in sent],
            tags=[token.tag_ for token in sent],
            # Get lemmas - ensure proper lemmatization
            # Lowercase lemmas for consistency, but preserve the lemmatized form
            lemmas=[token.lemma_.lower() if hasattr(token, 'lemma_') and token.lemma_ else token.text.lower() for token in sent],
            # FIXME: how to get SpaCy chunks?
            chunks=["O" for token in sent],
            entities=entities,  # Use BioC annotations, not spaCy NER
            norms=[token.text for token in sent],
            graphs=ConversionUtils.to_clu_graph(sent),
        )
        # FIXME: create hybrid graph
        return sentence

    @staticmethod
    def to_clu_document(doc: Doc) -> processors.Document:
        """
        Converts a SpaCy Doc object to a CLU Processors Document object.

        Parameters
        ----------
        doc: a SpaCy Doc object

        Returns
        -------
        document: a processors Document object
        """
        sentences = []
        for sent in doc.sents:
            sentences.append(ConversionUtils.to_clu_sentence(sent))
        return processors.Document(id=doc._.doc_id or "document", sentences=sentences)
