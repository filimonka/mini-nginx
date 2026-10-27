from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Upstream(ConfigModel):
    host: str
    port: int = Field(ge=1, le=65535)


class Timeouts(ConfigModel):
    connect_ms: int = Field(gt=0)
    read_ms: int = Field(gt=0)
    write_ms: int = Field(gt=0)
    total_ms: int = Field(gt=0)

    @field_validator("*", mode="before")
    @classmethod
    def validate_ms_to_s(cls, value: int) -> int:
        if not isinstance(value, int) or value <= 0:
            raise ValueError("must be greater than 0")
        return int(value / 1000)


class Limits(ConfigModel):
    max_client_conns: int = Field(gt=0)
    max_conns_per_upstream: int = Field(gt=0)


class Logging(ConfigModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    @field_validator("level", mode="before")
    @classmethod
    def validate_level(cls, value: object) -> object:
        if isinstance(value, str):
            value = value.upper()
        return value


class Config(BaseSettings):
    listen: str
    upstreams: list[Upstream] = Field(min_length=1)
    timeouts: Timeouts
    limits: Limits
    logging: Logging
    model_config = SettingsConfigDict(
        yaml_file=Path(__file__).with_name("config.yml"),
        yaml_file_encoding="utf-8",
        extra="forbid",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    def get_host_port_to_listen(self) -> tuple[str, int]:
        host, port_str = self.listen.split(":")
        return host, int(port_str)


config = Config()
