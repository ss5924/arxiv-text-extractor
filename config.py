import os

from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# DB 설정
POSTGRESQL_DB_URL = os.getenv("POSTGRESQL_DB_URL")
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

MAX_CONCURRENT_TASKS = 4
BATCH_SIZE = 100
