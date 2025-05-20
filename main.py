import asyncio
import aiohttp
import tempfile
import os
import logging
import re

from datetime import datetime
from pdfminer.high_level import extract_text

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import insert

from models import Paper
from database import async_session, mongo_collection

logging.basicConfig(level=logging.INFO)


def clean_text(text: str) -> str:
    # 페이지/헤더/푸터 제거
    text = re.sub(r"Page\s+\d+.*", "", text)
    text = re.sub(r"arXiv:\d{4}\.\d{4,5}(v\d+)?", "", text)

    # 하이픈 줄바꿈 및 단어 줄바꿈 연결
    text = re.sub(r"(?<=\w)-\n(?=\w)", "", text)
    text = re.sub(r"(?<=\w)\n(?=\w)", " ", text)

    # figure, table 라벨 제거
    text = re.sub(r"(Figure|Table) \d+:.*?(?=\n|$)", "", text)

    # section 번호 제거 (1, 2.1, 3.2.4 ...)
    text = re.sub(r"^\s*\d+(\.\d+)*\s+", "", text, flags=re.MULTILINE)

    # 이름/소속/이메일 제거
    text = re.sub(r"(?i)([a-z]+\s){2,7}((UC|MIT|Stanford|Berkeley|Google|Facebook|AI|Dept)[^\n]*)", "", text)

    # 줄바꿈 정리
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


async def log_failure(session: AsyncSession, arxiv_id, pdf_url, category, error_msg):
    stmt = insert(Paper.__table__.metadata.tables['arxiv_raw.failed_extract_papers']).values(
        arxiv_id=arxiv_id,
        pdf_url=pdf_url,
        category=category,
        error_msg=error_msg
    ).on_conflict_do_nothing(index_elements=["arxiv_id"])
    await session.execute(stmt)
    await session.commit()


async def process_pdf(arxiv_id: str, pdf_url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(pdf_url) as resp:
            if resp.status != 200:
                raise Exception(f"HTTP status {resp.status}")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(await resp.read())
                tmp_path = tmp_file.name

            text = extract_text(tmp_path)
            os.remove(tmp_path)
            return clean_text(text)


async def process_batch():
    BATCH_SIZE = 10
    offset = 0

    while True:
        async with async_session() as session:
            result = await session.execute(
                select(Paper.arxiv_id, Paper.pdf_url, Paper.category)
                .where(Paper.pdf_url.isnot(None))
                .offset(offset)
                .limit(BATCH_SIZE)
            )
            rows = result.all()

            if not rows:
                break

            logging.info(f"Loop start. offset: {offset}.")

            for arxiv_id, pdf_url, category in rows:
                if await mongo_collection.find_one({"arxiv_id": arxiv_id}):
                    continue

                for attempt in range(3):
                    try:
                        logging.info(f"[{arxiv_id}] Attempt count: {attempt + 1}")
                        text = await process_pdf(arxiv_id, pdf_url)
                        if not text:
                            raise Exception("Extract_failed")

                        await mongo_collection.update_one(
                            {"arxiv_id": arxiv_id},
                            {"$set": {
                                "arxiv_id": arxiv_id,
                                "pdf_url": pdf_url,
                                "cleaned_text": text,
                                "category": category
                            }},
                            upsert=True
                        )
                        logging.info(f"[{arxiv_id}] Complete.")
                        break

                    except Exception as e:
                        logging.warning(f"[{arxiv_id}] Failed: {e}")
                        if attempt == 2:
                            await log_failure(session, arxiv_id, pdf_url, category, str(e))

        offset += BATCH_SIZE


if __name__ == "__main__":
    asyncio.run(process_batch())
