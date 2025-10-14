from pathlib import Path
from typing import Literal

import yaml
from loguru import logger
from pydantic import Field

from core.sockets.actors.base import BaseActor
from core.sockets.types.envelope import Actor, AliasedBaseModel
from core.sockets.types.message import Message
from core.sockets.utils.streamer import stream_chunks_openai

from .. import sio

logger = logger.bind(name=__name__)

MODEL: Literal["gpt-4o", "gpt-5"] = "gpt-4o"

CURRENT_ACTOR: Actor = "scriptwriter"


class ScriptWritingRequest(AliasedBaseModel):
    prompt: str = Field(description="The prompt for the script writer")


class ScriptWritingActor(BaseActor[ScriptWritingRequest]):
    def __init__(self):
        super().__init__(
            actor_name="scriptwriter", model=MODEL, stream_chunks=stream_chunks_openai
        )

    def load_system_prompt_message(self) -> Message:
        with open(Path("core/prompts/teardown.yaml"), "r") as f:
            data = yaml.safe_load(f)
            system_prompt = data["story_writing_prompt"]
            logger.info(
                "System prompt",
                system_prompt=system_prompt[:100],
                length=len(system_prompt),
            )
            return Message(role="system", content=system_prompt)

    def prepare_messages(
        self, validated_request: ScriptWritingRequest
    ) -> list[Message]:
        system_prompt = self.load_system_prompt_message()
        return [system_prompt, Message(role="user", content=validated_request.prompt)]


@sio.on(f"c2s.{CURRENT_ACTOR}.stream.start")
async def handle_chat_stream_start(
    sid: str,
    envelope: dict,
) -> str:
    scriptwriting_actor = ScriptWritingActor()
    return scriptwriting_actor.handle_stream_start(
        sid, envelope, ScriptWritingRequest, sio
    )
