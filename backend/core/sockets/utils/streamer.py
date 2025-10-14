import uuid
from typing import TYPE_CHECKING, Any, Literal

from loguru import logger

if TYPE_CHECKING:
    from socketio import AsyncServer  # type: ignore[import-untyped]

from core.sockets.types.envelope import Actor, Envelope
from core.sockets.types.message import Message

from .. import async_openai_client

logger = logger.bind(name=__name__)

MODELS = Literal["gpt-4o", "gpt-5"]


async def stream_chunks_static_text(
    sid: str,
    text: str,
    actor: Actor,
    sio: "AsyncServer",
) -> str:
    stream_id, request_id = str(uuid.uuid4()), str(uuid.uuid4())
    text_chunks = text.split("\n")
    start_envelope = Envelope(
        request_id=request_id,
        stream_id=stream_id,
        seq=0,
        direction="s2c",
        actor=actor,
        action="stream",
        modifier="start",
        data={"delta": "start"},
    )
    await sio.emit(
        f"s2c.{actor}.stream.start",
        start_envelope.model_dump_json(),
        to=sid,
    )
    for seq, chunk in enumerate(text_chunks):
        envelope_to_send = Envelope(
            request_id=request_id,
            stream_id=stream_id,
            seq=seq,
            direction="s2c",
            actor=actor,
            action="stream",
            modifier="chunk",
            data={
                "delta": chunk,
            },
        )
        await sio.emit(
            f"s2c.{actor}.stream.chunk",
            envelope_to_send.model_dump_json(),
            to=sid,
        )
    end_envelope = Envelope(
        request_id=request_id,
        stream_id=stream_id,
        seq=len(text_chunks),
        direction="s2c",
        actor=actor,
        action="stream",
        modifier="end",
        data={"finish_reason": "stop"},
    )
    await sio.emit(
        f"s2c.{actor}.stream.end",
        end_envelope.model_dump_json(),
        to=sid,
    )
    return text


async def stream_chunks_openai(
    sid: str,
    data: list[Message],
    request_id: str,
    stream_id: str,
    actor: Actor,
    model: MODELS,
    sio: "AsyncServer",
) -> str:
    kwargs: dict[str, Any] = {}
    if model == "gpt-5":
        kwargs["reasoning_effort"] = "high"
    if model == "gpt-4o":
        kwargs["temperature"] = 0.7
    stream = await async_openai_client.chat.completions.create(
        model=model,
        messages=[msg.to_openai_message() for msg in data],
        stream=True,
        **kwargs,
    )
    accumulated_content = ""

    seq = 0
    async for chunk in stream:
        seq += 1
        if chunk.choices[0].delta.content is not None:
            accumulated_content += chunk.choices[0].delta.content
            envelope_to_send = Envelope(
                request_id=request_id,
                stream_id=stream_id,
                seq=seq,
                direction="s2c",
                actor=actor,
                action="stream",
                modifier="chunk",
                data={
                    "delta": chunk.choices[0].delta.content,
                },
            )
            await sio.emit(
                f"s2c.{actor}.stream.chunk",
                envelope_to_send.model_dump_json(),
                to=sid,
            )
        elif chunk.choices[0].finish_reason is not None:
            envelope_to_send = Envelope(
                request_id=request_id,
                stream_id=stream_id,
                seq=seq,
                direction="s2c",
                actor=actor,
                action="stream",
                modifier="end",
                data={
                    "finish_reason": chunk.choices[0].finish_reason,
                },
            )
            await sio.emit(
                f"s2c.{actor}.stream.end",
                envelope_to_send.model_dump_json(),
                to=sid,
            )
    return accumulated_content
