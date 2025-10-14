import uuid
from dataclasses import replace
from pathlib import Path
from typing import Literal

import yaml
from loguru import logger

from core.event_queue import ObservationUpdatedEvent
from core.observation_manager import ObservationManager
from core.sockets.types.envelope import Actor, Envelope
from core.sockets.types.message import Message
from core.sockets.utils.streamer import stream_chunks_openai

logger = logger.bind(name=__name__)


class ScriptWriter:
    def __init__(
        self,
    ) -> None:
        self.actor_name: Actor = "scriptwriter"
        self.model: Literal["gpt-4o", "gpt-5"] = "gpt-4o"

    async def write_script(self, user_prompt: str) -> str:
        return (
            "Consider this a story written by a scriptwriter, the user prompt is: "
            + user_prompt
        )

    async def handle_observation_updated(self, event: ObservationUpdatedEvent):
        observation_manager = ObservationManager()
        observation = event.observation
        if observation.user_prompt is None:
            logger.info(
                "Observation updated, but user prompt is None, not my time to shine."
            )
            return
        if observation.story is not None:
            logger.info("My job is done, I already wrote the story.")
            return
        logger.info("My time to shine, I'll write the story.")
        story = await self.write_script(observation.user_prompt)
        observation = replace(observation, story=story)
        await self.stream_openai(event)
        await observation_manager.update_observation(event.sid, observation, event.sio)

    def load_system_prompt_message(self) -> str:
        with open(Path("core/prompts/prompt_repo.yaml"), "r") as f:
            data = yaml.safe_load(f)
            system_prompt = data["story_writing_prompt"]
            logger.info(
                "System prompt",
                system_prompt=system_prompt[:100],
                length=len(system_prompt),
            )
            return system_prompt

    async def stream_openai(self, event: ObservationUpdatedEvent):
        sio = event.sio
        stream_id = str(uuid.uuid4())
        request_id = str(uuid.uuid4())
        start_envelope = Envelope(
            request_id=request_id,
            stream_id=stream_id,
            seq=0,
            direction="s2c",
            actor=self.actor_name,
            action="stream",
            modifier="start",
            data={"delta": "start"},
        )
        await sio.emit(
            f"s2c.{self.actor_name}.stream.start",
            start_envelope.model_dump_json(),
            to=event.sid,
        )
        data = [
            Message(role="system", content=self.load_system_prompt_message()),
            Message(role="user", content=event.observation.user_prompt or ""),
        ]
        await stream_chunks_openai(
            event.sid, data, request_id, stream_id, self.actor_name, self.model, sio
        )


scriptwriter = ScriptWriter()
