from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Enum, Text
import enum

__Base = declarative_base()


class FailedExtractPaper(AsyncAttrs, __Base):
    __tablename__ = "failed_extract_papers"
    __table_args__ = {"schema": "arxiv_raw"}

    arxiv_id = Column(String, primary_key=True)
    pdf_url = Column(String)
    category = Column(String)
    error_msg = Column(Text)


class TaskStatus(str, enum.Enum):
    pending = "pending"
    done = "done"
    fail = "fail"


class TaskQueue(AsyncAttrs, __Base):
    __tablename__ = "task_queue"
    __table_args__ = {"schema": "arxiv_raw"}

    id = Column(Integer, primary_key=True)
    start = Column(DateTime)
    status = Column(
        Enum(TaskStatus, name="status", native_enum=False),
        default=TaskStatus.pending
    )
    retries = Column(Integer, default=0)


class Paper(AsyncAttrs, __Base):
    __tablename__ = "papers"
    __table_args__ = {"schema": "arxiv_raw"}

    arxiv_id = Column(String, primary_key=True)
    authors = Column(String)
    category = Column(String)
    abstract = Column(String)
    published_date = Column(DateTime)
    pdf_url = Column(String)
