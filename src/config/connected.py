from src.config.aws_settings import AwsSettings
from dotenv import load_dotenv
import psycopg
import os
from urllib.parse import quote


class Settings:
    def __init__(self, prefix_name_base: str):
        load_dotenv()

        self.prefix_name_base = prefix_name_base
        self.DB_HOST = os.getenv(f'{self.prefix_name_base}_HOST')
        self.DB_DB_NAME = os.getenv(f'{self.prefix_name_base}_DB_NAME')
        self.DB_USER = os.getenv(f'{self.prefix_name_base}_USER')
        self.DB_PASSWORD = AwsSettings.get_storage_password(f'{self.prefix_name_base}_PASSWORD')
        self.DB_PORT = os.getenv(f'{self.prefix_name_base}_PORT')

        if None in [self.DB_HOST, self.DB_DB_NAME, self.DB_USER, self.DB_PASSWORD, self.DB_PORT]:
            raise ValueError("Some required environment variables are missing")

    def database_connect(self) -> psycopg.connect:
        return (
            f"host={self.DB_HOST} "
            f"dbname={self.DB_DB_NAME} "
            f"user={self.DB_USER} "
            f"password={self.DB_PASSWORD} "
            f"port={self.DB_PORT}"
        )

    @property
    def database_url(self) -> str:
        if self.prefix_name_base == 'SRC_M':
            return f"mysql+pymysql://{self.DB_USER}:{quote(self.DB_PASSWORD)}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DB_NAME}"
        else:
            return f"postgres://{self.DB_USER}:{quote(self.DB_PASSWORD)}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DB_NAME}"


