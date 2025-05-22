from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from config import POSTGRESQL_DB_URL, MONGO_DB_URL

# POSTGRESQL
__engine = create_async_engine(
    url=POSTGRESQL_DB_URL,
    pool_size=10,
    max_overflow=5,
    pool_timeout=30,
    echo=False
)
async_session = async_sessionmaker(__engine, expire_on_commit=False)

# MongoDB
__mongo_client = AsyncIOMotorClient(MONGO_DB_URL)
__mongo_db = __mongo_client["arxiv"]
mongo_collection = __mongo_db["papers"]
