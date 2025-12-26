from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_hostname: str
    database_port: str
    database_password: str
    database_name: str
    database_username: str
    app_env: str = "local"

    class Config:
        env_file = ".env"

settings = Settings()