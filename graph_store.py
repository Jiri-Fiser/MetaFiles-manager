from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, Tuple

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.compare import to_canonical_graph

from sqlalchemy import ForeignKey, Index, String, UniqueConstraint, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def canonical_graph_sha256(g: Graph) -> bytes:
    """
    Compute a stable SHA-256 digest of an RDF graph.

    The graph is first canonicalized (blank nodes are deterministically relabeled),
    then serialized to N-Triples, and finally hashed using SHA-256.

    Properties:
    - Isomorphic RDF graphs produce identical digests.
    - The output is a fixed-length 32-byte value.
    - Suitable for persistent storage and fast change detection.

    :param g: RDFLib Graph to hash.
    :return: 32-byte SHA-256 digest of the canonical graph.
    """
    nt: bytes = to_canonical_graph(g).serialize(format="nt").encode("utf8")
    return hashlib.sha256(nt).digest()


@lru_cache(maxsize=10000)
def parse_n3_term(n3: str):
    """
    Parse a single RDF term from its N3 representation using rdflib's public API.
    """
    g = Graph()
    # create a dummy triple and parse it
    data = f"<urn:x> <urn:y> {n3} ."
    g.parse(data=data, format="turtle")
    for _, _, o in g:
        return o
    raise ValueError(f"Could not parse N3 term: {n3}")


def _term_kind(term) -> str:
    if isinstance(term, URIRef):
        return "I"
    if isinstance(term, Literal):
        return "L"
    if isinstance(term, BNode):
        return "B"
    raise TypeError(f"Unsupported RDF term type: {type(term)!r}")


@dataclass
class RDFRelationalStore:
    """
    Helper that caches node IDs within a SQLAlchemy session so repeated store/load is fast.

    Important:
    - Caches are per-instance. Create one store per DB session-lifecycle (or reuse carefully).
    - Blank nodes are *not* cached globally by their original labels; instead we compress them
      per stored graph to _:b0, _:b1, ... and store those strings.
    """
    # pass your ORM classes here
    Node: type
    Triple: type

    # cache: (kind, n3_value) -> node_id
    _node_cache: Dict[Tuple[str, str], int] = field(default_factory=dict)


    def get_or_create_node_id(self, session: Session, kind: str, n3_value: str) -> int:
        """Return Node.id for (kind, n3_value); create row if missing."""
        key = (kind, n3_value)
        if key in self._node_cache:
            return self._node_cache[key]

        stmt = select(self.Node).where(self.Node.kind == kind, self.Node.n3_value == n3_value)
        node = session.execute(stmt).scalar_one_or_none()
        if node is None:
            node = self.Node(kind=kind, n3_value=n3_value)
            session.add(node)
            session.flush()

        node_id = int(node.id)
        self._node_cache[key] = node_id
        return node_id

    # ----------------- Storing --------------------------------------------

    def store_graph(
        self,
        session: Session,
        *,
        source_id: int,
        graph: Graph,
        clear_existing: bool = True,
    ) -> int:
        """
        Store a rdflib.Graph for a given source_ark.

        - BNodes are compressed per graph to _:b0, _:b1, ... (short & deterministic for this store call).
        - IRI/Literal are stored as full N3 using .n3() (no namespace manager => prefixes not required to parse).
        - If clear_existing=True, existing triples for the source are deleted first.
          (Nodes are not deleted; you can garbage-collect later if you ever want.)
        """

        if clear_existing:
            # delete triples for this graph/source
            session.query(self.Triple).filter(self.Triple.source_id == source_id).delete(synchronize_session=False)

        # Per-graph map: original BNode -> short label string (without _: prefix)
        bmap: Dict[BNode, str] = {}
        next_ix = 0

        def term_to_node_id(term) -> int:
            nonlocal next_ix

            k = _term_kind(term)

            if k == "B":
                # compress to short labels b0, b1, ...
                assert isinstance(term, BNode)
                short = bmap.get(term)
                if short is None:
                    short = f"b{next_ix}"
                    bmap[term] = short
                    next_ix += 1
                n3 = f"_:{short}"  # store as N3 bnode
                return self.get_or_create_node_id(session, "B", n3)

            # Store full N3 for IRI/Literal so it can be parsed without prefix context
            n3 = term.n3()  # IMPORTANT: no namespace_manager => <...> form, typed literals preserved
            return self.get_or_create_node_id(session, k, n3)

        # Insert triples
        to_add = []
        for s, p, o in graph:
            sid = term_to_node_id(s)
            pid = term_to_node_id(p)
            oid = term_to_node_id(o)

            to_add.append(self.Triple(
                source_id=source_id,
                subject_id=sid,
                predicate_id=pid,
                object_id=oid,
            ))

        session.add_all(to_add)
        session.flush()
        return source_id

    # ----------------- Loading --------------------------------------------

    def load_graph(
        self,
        session: Session,
        source_id: int,
    ) -> Graph:
        """
        Load a graph back into rdflib.Graph.

        Provide either source_ark or source_id.

        BNodes are re-created as unique-per-source by salting:
          stored "_:b0" -> BNode("b0g{source_id}")

        This keeps local consistency within the loaded graph and avoids accidental cross-graph identity.
        """

        # Fetch all triples for source
        rows = session.execute(
            select(self.Triple.subject_id, self.Triple.predicate_id, self.Triple.object_id)
            .where(self.Triple.source_id == source_id)
        ).all()

        if not rows:
            return Graph()

        # Collect node IDs
        node_ids = set()
        for sid, pid, oid in rows:
            node_ids.add(int(sid))
            node_ids.add(int(pid))
            node_ids.add(int(oid))

        # Fetch node values
        node_rows = session.execute(
            select(self.Node.id, self.Node.kind, self.Node.n3_value).where(self.Node.id.in_(node_ids))
        ).all()

        id_to_node: Dict[int, Tuple[str, str]] = {int(i): (str(k), str(v)) for i, k, v in node_rows}

        # Parse N3 back to rdflib terms, with bnode salting per source
        bnode_map: Dict[str, BNode] = {}

        def parse_node(node_id: int):
            kind, n3 = id_to_node[node_id]
            term = parse_n3_term(n3)  # returns URIRef/Literal/BNode
            if kind == "B":
                # term is BNode with some label (e.g. "b0")
                assert isinstance(term, BNode)
                label = str(term)  # "b0"
                salted = bnode_map.get(label)
                if salted is None:
                    salted = BNode(f"{label}g{source_id}")
                    bnode_map[label] = salted
                return salted
            return term

        g = Graph()
        for sid, pid, oid in rows:
            s = parse_node(int(sid))
            p = parse_node(int(pid))
            o = parse_node(int(oid))
            g.add((s, p, o))

        return g


