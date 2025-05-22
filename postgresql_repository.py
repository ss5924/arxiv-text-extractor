from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Paper, FailedExtractPaper


async def log_failure(session: AsyncSession, arxiv_id, pdf_url, category, error_msg):
    stmt = insert(FailedExtractPaper).values(
        arxiv_id=arxiv_id,
        pdf_url=pdf_url,
        category=category,
        error_msg=error_msg
    ).on_conflict_do_nothing(index_elements=["arxiv_id"])

    await session.execute(stmt)
    await session.commit()


async def fetch_papers(session: AsyncSession, offset, limit):
    result = await session.execute(
        select(Paper.arxiv_id, Paper.pdf_url, Paper.category)
        .where(Paper.pdf_url.isnot(None))
        .offset(offset)
        .limit(limit)
    )
    return result.all()
