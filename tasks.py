from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from models import Paper


async def log_failure(session: AsyncSession, arxiv_id, pdf_url, category, error_msg):
    stmt = insert(Paper.__table__.metadata.tables['arxiv_raw.failed_extract_papers']).values(
        arxiv_id=arxiv_id,
        pdf_url=pdf_url,
        category=category,
        error_msg=error_msg
    ).on_conflict_do_nothing(index_elements=["arxiv_id"])
    await session.execute(stmt)
    await session.commit()
