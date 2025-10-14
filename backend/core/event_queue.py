import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, Type

from loguru import logger

from core.observation import Observation
from core.singleton import SingletonMeta

logger = logger.bind(name=__name__)

if TYPE_CHECKING:
    from socketio import AsyncServer  # type: ignore[import-untyped]


@dataclass(frozen=True)
class ObservationUpdatedEvent:
    sid: str
    observation: Observation
    sio: "AsyncServer"


class EventBus(metaclass=SingletonMeta):
    def __init__(self):
        self.event_queue = asyncio.Queue()
        self.subscribers = []

    async def publish(self, event: ObservationUpdatedEvent):
        logger.info("Publishing event", event=event)
        await self.event_queue.put(event)
        logger.info("Event queued", event=event)
        for event_type, subscriber in self.subscribers:
            if isinstance(event, event_type):
                logger.info("Subscribing to event", event=event)
                await subscriber(event)
                logger.info("Event subscribed", event=event)

    def add_subscriber(
        self,
        event_type: Type[ObservationUpdatedEvent],
        subscriber_handler: Callable[[ObservationUpdatedEvent], Awaitable[None]],
    ):
        logger.info(
            "Adding subscriber",
            event_type=event_type,
            subscriber_handler=subscriber_handler,
        )
        self.subscribers.append((event_type, subscriber_handler))


def initialize_event_subscribers() -> None:
    from agents.scriptwriter import scriptwriter

    event_bus = EventBus()

    event_bus.add_subscriber(
        ObservationUpdatedEvent, scriptwriter.handle_observation_updated
    )
