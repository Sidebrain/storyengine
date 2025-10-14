import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from typing import Literal

from loguru import logger

from core.observation_manager import ObservationManager
from core.sockets.types.envelope import AliasedBaseModel, Envelope

from .. import sio

logger = logger.bind(name=__name__)

MODEL: Literal["gpt-4o", "gpt-5"] = "gpt-4o"


class DirectorRequest(AliasedBaseModel):
    prompt: str


@sio.on("c2s.director.stream.start")
async def handle_chat_stream_start(
    sid: str,
    envelope: dict,
) -> str:
    logger.info("Director request received")
    observation_manager = ObservationManager()
    logger.info("Observation manager created / retrieved")
    request = Envelope[DirectorRequest].model_validate(envelope)
    logger.info("Request validated")
    observation = observation_manager.get_observation(sid)
    logger.info("Observation retrieved", observation=observation)
    new_observation = replace(
        observation,
        user_prompt=request.data.prompt,
        updated_at=datetime.now(timezone.utc),
    )
    logger.info("New observation created", new_observation=new_observation)
    logger.info("Updating observation")
    asyncio.create_task(
        observation_manager.update_observation(sid, new_observation, sio)
    )
    logger.info("Observation updated")
    return "ack"
