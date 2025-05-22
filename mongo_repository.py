from database import mongo_collection


async def paper_exists(arxiv_id):
    return await mongo_collection.find_one({"arxiv_id": arxiv_id})


async def save_paper(arxiv_id, pdf_url, text, category):
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
