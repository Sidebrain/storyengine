import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Literal, Protocol, Type, TypeVar

from loguru import logger
from pydantic import BaseModel, ValidationError

from core.sockets.types.envelope import AckFail, AckOk, Actor, Envelope, Error
from core.sockets.types.message import Message

if TYPE_CHECKING:
    from socketio import AsyncServer  # type: ignore[import-untyped]

MODEL_TYPE = Literal["gpt-4o", "gpt-5"]
logger = logger.bind(name=__name__)

T = TypeVar("T", bound=BaseModel)


class StreamChunks(Protocol):
    async def __call__(
        self,
        sid: str,
        messages: list[Message],
        request_id: str,
        stream_id: str,
        actor: Actor,
        model: MODEL_TYPE,
        sio: "AsyncServer",
    ) -> None: ...


class BaseActor(ABC, Generic[T]):
    def __init__(
        self, actor_name: Actor, model: MODEL_TYPE, stream_chunks: StreamChunks
    ):
        self.actor_name = actor_name
        self.model = model
        self.stream_chunks = stream_chunks

    @abstractmethod
    def prepare_messages(self, validated_request: T) -> list[Message]: ...

    def handle_stream_start(
        self, sid: str, envelope: dict, data_type: Type[T], sio: "AsyncServer"
    ) -> str:
        try:
            validated_envelope = Envelope[data_type].model_validate(envelope)  # type: ignore

            if validated_envelope.request_id is None:
                return AckFail(
                    ok=False,
                    error=Error(
                        code="invalid_envelope",
                        message="The envelope is missing request_id",
                    ),
                ).model_dump_json()

        except ValidationError:
            return AckFail(
                ok=False,
                error=Error(
                    code="invalid_envelope",
                    message="The envelope is not in the correct format",
                ),
            ).model_dump_json()

        stream_id = str(uuid.uuid4())

        prepared_messages = self.prepare_messages(validated_envelope.data)

        asyncio.create_task(
            self.stream_chunks(
                sid,
                prepared_messages,
                validated_envelope.request_id,
                stream_id,
                actor=self.actor_name,
                model=self.model,
                sio=sio,
            )
        )

        return AckOk(
            ok=True,
            request_id=validated_envelope.request_id,
            stream_id=stream_id,
        ).model_dump_json()
