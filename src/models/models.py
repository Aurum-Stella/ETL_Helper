from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import Session, registry, declarative_base, as_declarative, mapped_column, Mapped
from src.config.connected import get_test_engine
from src.utils.custom_type import *


# tablesUsed = ['alembic_version']
tablesUsed = ['events', 'test_table']

mapper_registry = registry()
mapper_registry.metadata.reflect(get_test_engine(),
                                 only=tablesUsed)
Base = mapper_registry.generate_base()

class TestTable(Base):
    __tablename__ = 'test_table'
    __table_args__ = {'extend_existing': True} # Разрешает изменение таблицы
    id: Mapped[uuidpk]
    # user_id: Mapped[int] = mapped_column(ForeignKey(column='clients.id'))
    user_id: Mapped[int] = mapped_column()
    user_name: Mapped[str] = mapped_column()

# alembic revision --autogenerate -m "Migration name"
# alembic upgrade head