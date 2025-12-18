from __future__ import annotations
from clu.bridge.typing import Tokens, Indices
from enum import Enum
from typing import Dict, ForwardRef, List, Optional, Text, Tuple
from pydantic import BaseModel, Extra, Field, PrivateAttr, validate_arguments
from clu.bridge import odinson

__all__ = ["Document", "Sentence", "DirectedGraph", "Graphs", "Edge"]

GraphMap = Dict[ForwardRef("Graphs"), ForwardRef("DirectedGraph")]

# see https://github.com/clulab/processors/blob/master/main/src/main/scala/org/clulab/struct/GraphMap.scala
class Graphs(Text, Enum):
    UNIVERSAL_BASIC = "universal-basic"  # basic Universal dependencies
    UNIVERSAL_ENHANCED = (
        "universal-enhanced"  # collapsed (or enhanced) Universal dependencies
    )
    STANFORD_BASIC = "stanford-basic"  # basic Stanford dependencies
    STANFORD_COLLAPSED = "stanford-collapsed"  # collapsed Stanford dependencies
    SEMANTIC_ROLES = "semantic-roles"  # semantic roles from CoNLL 2008-09, which includes PropBank and NomBank
    ENHANCED_SEMANTIC_ROLES = "enhanced-semantic-roles"  # enhanced semantic roles
    HYBRID_DEPENDENCIES = (
        "hybrid"  # graph that merges ENHANCED_SEMANTIC_ROLES and UNIVERSAL_ENHANCED
    )


class Edge(BaseModel):
    source: int
    destination: int
    relation: Text

    def __hash__(self):
        return hash((self.source, self.destination, self.relation))


class DirectedGraph(BaseModel):
    edges: List[Edge]
    roots: List[int]

    def __hash__(self):
        return hash(tuple([hash(e) for e in self.edges] + [hash(self.roots)]))


class Sentence(BaseModel):
    raw: Tokens
    startOffsets: Indices
    endOffsets: Indices
    words: Optional[Tokens] = None
    tags: Optional[Tokens] = None
    lemmas: Optional[Tokens] = None
    entities: Optional[Tokens] = None
    chunks: Optional[Tokens] = None
    norms: Optional[Tokens] = None
    graphs: Optional[Dict[Graphs, DirectedGraph]] = None
    # tell pydantic to use enum *values*
    class Config:
        use_enum_values = True
        underscore_attrs_are_private = True
        validate_assignment = True
        extra = Extra.allow  # "ignore" # vs. "allow"

    def __hash__(self):
        return hash(
            tuple(
                [
                    tuple(elem)
                    for elem in [
                        self.raw,
                        self.startOffsets,
                        self.endOffsets,
                        self.words,
                        self.tags,
                        self.lemmas,
                        self.entities,
                        self.chunks,
                        self.norms,
                        self.graphs,
                    ]
                ]
            )
        )


class Document(BaseModel):
    """clu.processors.Document"""

    id: Optional[Text] = None
    text: Optional[Text] = None
    sentences: List[Sentence]

    def __hash__(self):
        return hash(tuple([self.id, self.text] + [hash(s) for s in self.sentences]))

    class Config:
        use_enum_values = True
        underscore_attrs_are_private = True
        validate_assignment = True
        extra = Extra.allow  # "ignore" # vs. "allow"


class ConversionUtils:

    """Conversion utilities for processors to Odinson"""

    @staticmethod
    def _make_collapsed_deps(words: List[Text], edges: List[Edge]) -> Set[Edge]:
        """Converts prep -> pobj to prep_pobj edge"""
        PartialGraph = Dict[int, List[Tuple[int, Text]]]
        incoming: PartialGraph = {}
        outgoing: PartialGraph = {}
        for edge in edges:
            outgoing[edge.source] = outgoing.get(edge.source, []) + [
                (edge.destination, edge.relation)
            ]
            incoming[edge.destination] = incoming.get(edge.destination, []) + [
                (edge.source, edge.relation)
            ]
        res = set()
        for edge in edges:
            if edge.relation == "prep":
                adpos_idx = edge.destination
                # Use .get() to handle cases where destination has no outgoing edges
                for (dest, rel) in outgoing.get(edge.destination, []):
                    if rel == "pobj":
                        collapsed = Edge(
                            source=edge.source,
                            destination=dest,
                            relation=f"prep_{words[adpos_idx].lower()}",
                        )
                        res.add(collapsed)
        return res

    # - create processors.Graphs.HYBRID_DEPENDENCIES
    @staticmethod
    def _make_hybrid_graph(
        tokens: List[Text], graph_map: Dict[Graphs, DirectedGraph]
    ) -> DirectedGraph:
        """Combine all roots and edges of graphs, as well as collapsed edges"""
        if Graphs.HYBRID_DEPENDENCIES in graph_map:
            return graph_map[Graphs.HYBRID_DEPENDENCIES]

        edges: Set[Edge] = set()
        roots: Set[int] = set()
        for dg in graph_map.values():
            for root in dg.roots:
                roots.add(root)
            for edge in dg.edges:
                edges.add(edge)
        collapsed = ConversionUtils._make_collapsed_deps(
            words=tokens, edges=list(edges)
        )
        return DirectedGraph(roots=list(roots), edges=list(collapsed.union(edges)))

    @staticmethod
    def to_odinson_document(doc: Document) -> odinson.Document:
        """Create an OdinsonDocument from a processors.Document"""
        odinson_ss = [ConversionUtils.to_odinson_sentence(s) for s in doc.sentences]
        return odinson.Document(
            id=doc.id or str(doc.__hash__()), metadata=[], sentences=odinson_ss
        )

    @staticmethod
    def to_odinson_sentence(s: Sentence) -> odinson.Sentence:
        """Create an OdinsonSentence from a processors.Sentence"""
        graph = ConversionUtils._make_hybrid_graph(tokens=s.words, graph_map=s.graphs)
        return odinson.Sentence(
            numTokens=len(s.raw),
            # List[Type[Field]]
            fields=[
                f
                for f in [
                    # raw
                    odinson.TokensField(tokens=s.raw, name="raw"),
                    # dependencies (hybrid graph)
                    None
                    if s.graphs is None
                    else odinson.GraphField(
                        roots=graph.roots,
                        edges=[
                            (edge.source, edge.destination, edge.relation)
                            for edge in graph.edges
                        ],
                    ),
                    # words
                    None
                    if s.words is None
                    else odinson.TokensField(tokens=s.words, name="word"),
                    # lemmas
                    None
                    if s.lemmas is None
                    else odinson.TokensField(tokens=s.lemmas, name="lemma"),
                    # tags
                    None
                    if s.tags is None
                    else odinson.TokensField(tokens=s.tags, name="tag"),
                    # entities
                    None
                    if s.entities is None
                    else odinson.TokensField(tokens=s.entities, name="entity"),
                    # chunks
                    None
                    if s.chunks is None
                    else odinson.TokensField(tokens=s.chunks, name="chunk"),
                    # norms
                    None
                    if s.norms is None
                    else odinson.TokensField(tokens=s.norms, name="norm"),
                ]
                if f is not None
            ],
        )

    @staticmethod
    def to_processors_document(doc: odinson.Document) -> Document:
        return Document(
            id=doc.id,
            sentences=[
                ConversionUtils.to_processors_sentence(s) for s in doc.sentences
            ],
        )

    @staticmethod
    def create_character_offsets(toks: Tokens) -> Tuple[Indices, Indices]:
        """Create start and end char offsets for tokens by treating them as whitespace-delimited"""
        current_start = -1
        current_end = 0
        start_offsets = []
        end_offsets = []
        for tok in toks:
            current_start += 1
            start_offsets.append(current_start)
            current_start += len(tok)
            current_end += len(tok)
            end_offsets.append(current_end)
            current_end += 1
        return start_offsets, end_offsets

    @staticmethod
    def to_processors_sentence(s: odinson.Sentence) -> Sentence:

        graphs: Optional[GraphMap] = None
        # NOTE: by convention, these are non-plural
        fields_dict: Dict[Text, Optional[Tokens]] = {
            "raw": None,
            "word": None,
            "tag": None,
            "lemma": None,
            "entity": None,
            "chunk": None,
            "norm": None,
        }

        def is_token_field(
            field: odinson.Field, name: Optional[odinson.Fields] = None
        ) -> bool:
            _is_token_field = isinstance(field, odinson.TokensField)
            if name is not None:
                return True if _is_token_field and field.name == name else False
            return _is_token_field

        for field in s.fields:
            if is_token_field(field) and field.name in fields_dict:
                fields_dict[field.name] = field.tokens
            elif isinstance(field, odinson.GraphField):
                # assume graph is hybrid
                graphs = graphs or dict()
                graphs[Graphs.HYBRID_DEPENDENCIES] = DirectedGraph(
                    edges=[
                        Edge(source=e[0], destination=e[1], relation=e[2])
                        for e in field.edges
                    ],
                    roots=list(field.roots),
                )

        PLACEHOLDER = [""] * s.numTokens
        raw = (
            fields_dict.get("raw", PLACEHOLDER)
            or fields_dict.get("word", PLACEHOLDER)
            or PLACEHOLDER
        )
        start_offsets, end_offsets = ConversionUtils.create_character_offsets(raw)
        return Sentence(
            raw=raw,
            startOffsets=start_offsets,
            endOffsets=end_offsets,
            words=fields_dict.get("word", PLACEHOLDER) or PLACEHOLDER,
            tags=fields_dict.get("tag", None),
            lemmas=fields_dict.get("lemma", None),
            entities=fields_dict.get("entity", None),
            chunks=fields_dict.get("chunk", None),
            norms=fields_dict.get("norm", None),
            graphs=graphs,
        )
