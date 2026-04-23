from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Tuple, Type, TypeVar

from sqlalchemy import Integer, Text, create_engine, UnicodeText, select
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.inspection import inspect as sa_inspect

from sqlalchemy import DateTime, Enum as SAEnum, Index, String, Text, func
from policy import ConflictAction

class Severity(Enum):
    """Severity level of the log entry."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

class Base(DeclarativeBase): pass

class ChangeLog(Base):
    """
    Table for recording changes to ORM objects.

    Columns
    -------
    id : int
        Primary key, autoincremented.
    created_at : datetime
        Timestamp when the log entry was created (defaults to server time).
    object_id : str
        Identifier of the affected object (may be a numeric PK, UUID, or any string).
    attribute : str
        Name of the changed attribute/column.
    operation : str
        Performed operation ('add', 'update', 'delete').
    severity : Severity
        Severity of the change (info, warning, or error).
    old_value : str | None
        Previous value (stored as string).
    new_value : str | None
        New value (stored as string).
    description : str | None
        Optional description (reason, context, etc.).
    """
    __tablename__ = "change_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    object_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        index=True,
        doc="Identifier of the affected object (PK, UUID, etc.).",
    )

    attribute: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        doc="Name of the changed attribute/column.",
    )

    operation: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        doc="type of operation (insert, update, delete).",
    )

    severity: Mapped[Severity] = mapped_column(
        SAEnum(Severity, name="severity_enum"),
        nullable=False,
        default=Severity.INFO,
        server_default=Severity.INFO.value,
    )

    old_value: Mapped[Optional[str]] = mapped_column(UnicodeText, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(UnicodeText, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(UnicodeText, nullable=True)

    __table_args__ = (
        Index("ix_change_log_obj_attr_time", "object_id", "attribute", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<ChangeLog id={self.id} object_id={self.object_id!r} "
            f"attr={self.attribute!r} operation={self.operation} severity={self.severity.value} "
            f"at={self.created_at}>"
        )

def log_change(
    session: Session,
    *,
    object_id: str,
    attribute: str = None,
    operation: str,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    description: Optional[str] = None,
    severity: Severity = Severity.INFO,
) -> ChangeLog:
    """
    Create and add a new entry to the `ChangeLog` table.
    The caller is responsible for committing the session.

    Parameters
    ----------
    session : Session
        Open SQLAlchemy session.
    object_id : str
        Identifier of the affected object (string, PK, UUID, etc.).
    attribute : str
        Name of the changed column/attribute.
    operation : str
        Performed operation ('add', 'update', 'delete').
    old_value : str | None
        Previous value (stringified).
    new_value : str | None
        New value (stringified).
    description : str | None, optional
        Optional human-readable description (reason, context).
    severity : Severity, default INFO
        Severity of the change (info, warning, error).

    Returns
    -------
    ChangeLog
        The newly created ORM object (already added to the session).
    """
    entry = ChangeLog(
        object_id=object_id,
        attribute=attribute,
        operation=operation,
        severity=severity,
        old_value=old_value,
        new_value=new_value,
        description=description,
    )
    session.add(entry)
    return entry


T = TypeVar("T")

def _iter_column_attr_names(model_cls: Type[Any]) -> Iterable[str]:
    """Vrátí jména atributů, které odpovídají *přímo mapovaným sloupcům* (bez relací)."""
    mapper = sa_inspect(model_cls)
    for attr in mapper.column_attrs:
        # Přeskočíme sloupce, které jsou jen „read-only“ nebo server-default? Většinou je chceme zahrnout.
        yield attr.key


def upsert_with_policy(
    session: Session,
    instance: T,
    policy: Dict[str, ConflictAction],
    *,
    default_policy: ConflictAction = ConflictAction.STRICT,
    logger: Optional[Any] = None,
    log_description: Optional[str] = None,
) -> Tuple[str, T]:
    """
    Vloží nebo aktualizuje ORM objekt podle politiky konfliktů.

    Pravidla:
      1) Pokud v tabulce (dle PK) záznam neexistuje → vloží se (session.add).
      2) Pokud existující řádek je *identický* (všechny mapované sloupce shodné) → neudělá nic.
      3) Pokud existuje a liší se, pro každý *lišící se* sloupec:
         - IGNORE  : ponechá stávající hodnotu (nepřepíše ji).
         - WARNING : přepíše na novou hodnotu a zaloguje varování (pokud je k dispozici logger).
         - STRICT  : vyhodí StrictConflictError (nic se neprovede).
         - UPDATE  : přepíše na novou hodnotu (tichý update).
         Pro sloupce, které nejsou ve slovníku `policy`, se použije `default_action` (výchozí UPDATE).

    Poznámky:
      - Funkce *neprovádí* commit. Ten si řiď volající (umožňuje batch operace).
      - Identitu určujeme podle primárního klíče (session.get).
      - Porovnávají se jen *přímo mapované sloupce* (bez relací).
      - Vracená hodnota je dvojice (status, objekt), kde status je 'inserted' | 'unchanged' | 'updated'.

    Parameters
    ----------
    session : Session
        Otevřená SQLAlchemy session.
    instance : T
        ORM instance s nastaveným primárním klíčem.
    policy : Dict[str, ConflictAction]
        Mapa jméno_atributu -> akce při rozdílu.
    default_policy : ConflictAction, optional
        Akce pro atributy, které nejsou v `policy`. Default je Strict.
    logger : Optional[Any], optional
        Logger (např. logging.getLogger(...)). Pokud je None a akce je WARNING, varování se vytiskne printem.

    Returns
    -------
    Tuple[str, T]
        ('inserted' | 'unchanged' | 'updated', objekt_v_session)

    Raises
    ------
    StrictConflictError
        Pokud některý rozdílný atribut má politiku STRICT.
    """
    model_cls = instance.__class__
    key = model_cls.__natural_key__
    value = getattr(instance, key)

    stmt = select(model_cls).where(getattr(model_cls, key) == value)
    existing = session.scalar(stmt)
    pk = instance.ark

    # 1) neexistuje → vlož
    if existing is None:
        session.add(instance)
        log_change(session, object_id=str(pk), operation="INSERTED", severity=Severity.INFO)
        return "inserted", instance

    # 2/3) existuje → porovnej sloupce
    updated = False
    attr_names = tuple(_iter_column_attr_names(model_cls))

    # Zjistíme, zda jsou *všechny* sloupce identické (rychlá cesta).
    all_equal = True
    for name in attr_names:
        old = getattr(existing, name)
        new = getattr(instance, name)
        if old != new:
            all_equal = False
            break

    if all_equal:
        return "unchanged", existing

    instance.id = existing.id

    # Aplikuj politiku po sloupcích
    for name in attr_names:
        old = getattr(existing, name)
        new = getattr(instance, name)
        if old == new:
            continue  # žádný rozdíl

        action = policy.get(name, default_policy)

        if action is ConflictAction.STRICT:
            log_change(session, object_id=str(pk), operation="UPDATED", attribute=name, old_value=str(old), new_value=str(new),
                       severity=Severity.ERROR)
            continue

        elif action is ConflictAction.IGNORE:
            # ponecháme starou hodnotu → nic neděláme
            continue
        elif action is ConflictAction.WARNING:
            log_change(session, object_id=str(pk), operation="UPDATED", attribute=name, old_value=str(old), new_value=str(new),
                       severity=Severity.WARNING)
            msg = (
                f"WARNING: updating {model_cls.__name__}.{name} "
                f"from {old!r} to {new!r} for PK={pk}"
            )
            if logger is not None:
                logger.warning(msg)
            else:
                print(msg)
            setattr(existing, name, new)
            updated = True
        elif action is ConflictAction.UPDATE:
            setattr(existing, name, new)
            updated = True
        else:
            assert False, "unknown conflict action"

    return ("updated" if updated else "unchanged"), existing


if __name__ == "__main__":

    class User(Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        username: Mapped[str] = mapped_column(String(50))
        email: Mapped[str] = mapped_column(String(120))
        role: Mapped[str] = mapped_column(String(20), default="user")


    # Politika: username – STRICT (nesmí se změnit), email – WARNING (přepiš a varuj),
    # ostatní sloupce – default UPDATE
    policy = {
        "username": ConflictAction.STRICT,
        "email": ConflictAction.WARNING,
    }

    # vytvoří engine pro SQLite (soubor v aktuálním adresáři)
    engine = create_engine("sqlite:///mydb.sqlite3", echo=False)
    Base.metadata.create_all(engine)

    # připraví továrnu na session
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    # použití:
    with SessionLocal() as session:  # type: Session
        status, obj = upsert_with_policy(session, User(id=1, username="jiri2", email="new@ujep.cz", role="user1"),
                                         policy, default_policy=ConflictAction.STRICT)
        session.commit()
        print(status)




