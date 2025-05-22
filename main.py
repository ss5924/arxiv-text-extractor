import asyncio
import logging

import database
import logging_config
import mongo_repository
import pdf_manager
import postgresql_repository
from config import MAX_CONCURRENT_TASKS, BATCH_SIZE

logging_config.setup_logger()
logger = logging.getLogger(__name__)

semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


async def process_single_paper(session, arxiv_id, pdf_url, category):
    if await mongo_repository.paper_exists(arxiv_id):
        return

    async with semaphore:
        for attempt in range(3):
            try:
                logger.info(f"{arxiv_id} | Attempt {attempt + 1}")
                text = await pdf_manager.process_pdf(arxiv_id, pdf_url)

                if not text:
                    raise Exception("PDF extraction failed.")

                await mongo_repository.save_paper(arxiv_id, pdf_url, text, category)
                logger.info(f"{arxiv_id} | Processed successfully.")
                break

            except Exception as e:
                logger.warning(f"{arxiv_id} | Failed: {e}")
                if attempt == 2:
                    await postgresql_repository.log_failure(
                        session, arxiv_id, pdf_url, category, str(e)
                    )


async def run_batches():
    offset = 0

    while True:
        async with database.async_session() as session:
            rows = await postgresql_repository.fetch_papers(session, offset, BATCH_SIZE)

            if not rows:
                break

            logger.info(f"Batch start | Offset: {offset} | Rows: {len(rows)}")

            tasks = [
                process_single_paper(session, arxiv_id, pdf_url, category)
                for arxiv_id, pdf_url, category in rows
            ]
            await asyncio.gather(*tasks)

        offset += BATCH_SIZE


def main():
    asyncio.run(run_batches())


if __name__ == "__main__":
    main()
