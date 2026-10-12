from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


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


class Config(ConfigModel):
    listen: str
    upstreams: list[Upstream] = Field(min_length=1)
    timeouts: Timeouts
    limits: Limits
    logging: Logging

    def get_host_port_to_listen(self) -> tuple[str, int]:
        host, port_str = self.listen.split(":")
        return host, int(port_str)


raw = yaml.safe_load(Path("proxy/config.yml").read_text(encoding="utf-8"))
config = Config.model_validate(raw)
