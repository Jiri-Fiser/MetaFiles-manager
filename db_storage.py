from typing import Dict, ChainMap

from rdflib import Graph
from rdflib.compare import graph_diff

from db_tool import Base, ConflictAction, upsert_with_policy, log_change, Severity

from sqlalchemy import (
    create_engine,
    Text,
    LargeBinary,
    DateTime,
    UniqueConstraint,
    String,
    Index,
    ForeignKey, Integer
)

from sqlalchemy.orm import Mapped, mapped_column, Session, sessionmaker
from datetime import datetime

from graph_store import RDFRelationalStore


def get_session(db_url: str):
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session()

def initialize_database(db_url: str) -> sessionmaker:
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)

class ReprMixin:
    # Seznam dvojic (datový typ, formátovací funkce)
    __repr_formatters__ = [
        (datetime, lambda dt: dt.strftime("%Y-%m-%d %H:%M")),  # bez sekund
        (bytes, lambda b: b.hex())  # hexadecimální výpis
    ]

    def __repr__(self):
        values = []
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            for data_type, formatter in self.__repr_formatters__:
                if isinstance(value, data_type):
                    value = formatter(value)
                    break
            values.append(f"{column.name}={value!r}")
        return f"<{self.__class__.__name__}({', '.join(values)})>"

def upsert_with_graph(record, *, graph: Graph, session:Session, policy: Dict[str, ConflictAction]) :
    with session.begin():
        policy = dict(ChainMap({"created": ConflictAction.IGNORE,
                                "updated": ConflictAction.UPDATE,
                                "digest_meta": ConflictAction.UPDATE}, policy))
        old_digest = record.digest_meta
        action, instance = upsert_with_policy(session, record, policy)
        #save_graph(graph, old_digest=old_digest_meta, new_digest=instance.digest_meta, ark=self.ark,
        #           session=session, action=action, id=instance.id, policy=policy)
        store = RDFRelationalStore(Node=Node, Triple=Triple)

        if action == "inserted":
            session.flush()
            store.store_graph(session, source_id=instance.id, graph=graph, clear_existing=False)
            # log_change(session, object_id=repr(self.ark), attribute="metadata", operation="INSERTED", severity=Severity.INFO)
            return
        change: bool = old_digest != instance.digest_meta
        if not change:
            return

        saved_g = store.load_graph(session, instance.id)
        metadata_policy = policy.get("metadata", ConflictAction.STRICT)
        in_both, only_old, only_new = graph_diff(saved_g, graph)
        if metadata_policy in [ConflictAction.WARNING, ConflictAction.STRICT]:
            log_change(session, object_id=repr(record.ark), operation="UPDATED",
                       attribute="metadata",
                       old_value=only_old.serialize(format="turtle"),
                       new_value=only_new.serialize(format="turtle"),
                       severity=Severity.ERROR if metadata_policy == ConflictAction.STRICT else Severity.WARNING)
        if metadata_policy in [ConflictAction.IGNORE, ConflictAction.WARNING]:
            store.store_graph(session, source_id=instance.id, graph=graph, clear_existing=True)


class FileRecord(ReprMixin, Base):
    __tablename__ = "file_records"
    __table_args__ = (UniqueConstraint("local_path"),)
    __natural_key__ = "ark"

    id: Mapped[int] = mapped_column(primary_key=True)
    ark: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    storage: Mapped[int] = mapped_column(Integer, nullable=False)
    local_path: Mapped[str] = mapped_column(Text, nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    digest_meta: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def upsert_with_policy(self, session: Session, policy: Dict[str, ConflictAction], graph: Graph):
        upsert_with_graph(self, graph=graph, session=session, policy=policy)



class Node(ReprMixin, Base):
    __tablename__ = "nodes"

    id: Mapped[int] = mapped_column(primary_key=True)
    kind: Mapped[str] = mapped_column(String(1), nullable=False)  # 'I'|'L'|'B'
    n3_value: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("kind", "n3_value", name="uq_node_kind_n3"),
        Index("ix_node_kind_n3", "kind", "n3_value"),
    )


class Triple(ReprMixin, Base):
    __tablename__ = "triple"

    source_id: Mapped[int] = mapped_column(ForeignKey("file_records.id", ondelete="CASCADE"), primary_key=True)
    subject_id: Mapped[int] = mapped_column(ForeignKey("nodes.id"), primary_key=True)
    predicate_id: Mapped[int] = mapped_column(ForeignKey("nodes.id"), primary_key=True)
    object_id: Mapped[int] = mapped_column(ForeignKey("nodes.id"), primary_key=True)

    __table_args__ = (
        Index("ix_triple_src_sp", "source_id", "subject_id", "predicate_id"),
        Index("ix_triple_src_po", "source_id", "predicate_id", "object_id"),
    )

