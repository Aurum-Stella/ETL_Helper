import datetime
from typing import Annotated
from sqlalchemy import Table, Column, Integer, String, ForeignKey, text, DateTime
from sqlalchemy.orm import sessionmaker, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from typing import Annotated
import uuid


intpk = Annotated[int, mapped_column(primary_key=True)]
uuidpk = Annotated[uuid.UUID, mapped_column(pgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)]
created_on_record = Annotated[datetime.datetime, mapped_column(DateTime(timezone=True), server_default=text("TIMEZONE('Europe/Kiev', now())"))]
created_on = Annotated[datetime.datetime, mapped_column(DateTime(timezone=True))]