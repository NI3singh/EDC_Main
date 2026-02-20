from motor.motor_asyncio import AsyncIOMotorClient
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MONGODB_URL: str
    DB_NAME: str

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


class MongoConn:
    client: AsyncIOMotorClient | None = None

    def connect(self) -> None:
        self.client = AsyncIOMotorClient(settings.MONGODB_URL)

    def close(self) -> None:
        if self.client:
            self.client.close()

    def db(self):
        if not self.client:
            raise RuntimeError("MongoDB client not initialised")
        return self.client[settings.DB_NAME]


mongo = MongoConn()
