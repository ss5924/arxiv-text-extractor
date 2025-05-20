import asyncio

import logging

from sqlalchemy.future import select

from models import Paper
from database import async_session, mongo_collection
from tasks import log_failure
from pdf_manager import process_pdf

logging.basicConfig(level=logging.INFO)


async def process_batch():
    BATCH_SIZE = 100
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
