from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from loguru import logger

logger = logger.bind(name=__name__)

if TYPE_CHECKING:
    pass  # type: ignore[import-untyped]


@dataclass(frozen=True)
class Observation:
    created_at: datetime = datetime.now(timezone.utc)
    updated_at: datetime = datetime.now(timezone.utc)
    user_prompt: str | None = None
    story: str | None = None
