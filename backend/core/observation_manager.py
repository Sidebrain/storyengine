from typing import TYPE_CHECKING

from loguru import logger

from core.event_queue import EventBus, ObservationUpdatedEvent
from core.observation import Observation
from core.singleton import SingletonMeta

logger = logger.bind(name=__name__)

if TYPE_CHECKING:
    from socketio import AsyncServer  # type: ignore[import-untyped]


class ObservationManager(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self.event_bus = EventBus()
        self.observations: dict[str, Observation] = {}

    def add_observation(self, sid: str, observation: Observation):
        self.observations[sid] = observation

    def get_observation(self, sid: str) -> Observation:
        observation = self.observations.get(sid)
        if observation is None:
            new_observation = Observation()
            self.add_observation(sid, new_observation)
            return new_observation
        return observation

    async def update_observation(
        self, sid: str, observation: Observation, sio: "AsyncServer"
    ):
        self.observations[sid] = observation
        event = ObservationUpdatedEvent(
            sid=sid,
            observation=observation,
            sio=sio,
        )
        logger.info("Event created", event=event)
        await self.event_bus.publish(event)

    def delete_observation(self, sid: str):
        self.observations.pop(sid, None)
