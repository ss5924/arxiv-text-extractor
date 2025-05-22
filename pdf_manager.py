import aiohttp
import tempfile
import os

from pdfminer.high_level import extract_text
import text_preprocessor


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
            return text_preprocessor.clean_text(text)
