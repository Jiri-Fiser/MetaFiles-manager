from configparser import ConfigParser
from pathlib import Path
from typing import Iterable, Mapping

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session

from db_storage import FileRecord, Node, Triple
from graph_store import RDFRelationalStore
from json_tool import write_json_array_from_iterable


def filerecord_iterator(session: Session) -> Iterable[FileRecord]:
    stmt = select(FileRecord).execution_options(yield_per=128)
    for record in session.scalars(stmt):
        yield record


def export_iter(db_url: str) -> Iterable[Mapping[str, str]]:
    engine = create_engine(db_url, future=True)
    # noinspection PyPep8Naming
    SessionLocal = sessionmaker(engine)
    session: Session = SessionLocal()
    store = RDFRelationalStore(Node=Node, Triple=Triple)
    for record in filerecord_iterator(session):
        print(record.ark)
        print(record.local_path)
        g=store.load_graph(session, record.id)
        xml_str = g.serialize(format="xml")
        yield dict(ark=record.ark, metadata=xml_str)

def export(db_url: str, export_path: Path) -> None:
    with open(export_path, "wt", encoding="utf-8") as f:
        write_json_array_from_iterable(export_iter(db_url), f)


if __name__ == "__main__":
    config = ConfigParser()
    config.read("config.ini")
    #ki_naan = config["Storage"]["Naan"]
    location = config[config["Storage"]["Location"]]
    #path = Path(location["Path"])
    metafiles_db = location["Database"]
    #log = location["Log"]
    export_file = location["Export"]
    export(metafiles_db, Path(export_file))