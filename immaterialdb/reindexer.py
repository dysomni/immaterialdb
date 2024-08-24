import json
from abc import ABC, abstractmethod
from typing import Annotated, Literal

import ulid
from pydantic import BaseModel, Field, RootModel

from immaterialdb.config import RootConfig
from immaterialdb.constants import LOGGER
from immaterialdb.query import AllQuery


class QueueForModel(BaseModel):
    index_job_id: str
    command: Literal["queue_for_model"]
    entity_name: str
    last_evaluated_key: str
    batch_size: int


class ReindexEntity(BaseModel):
    index_job_id: str
    command: Literal["reindex_entity"]
    entity_name: str
    entity_id: str


Command = RootModel[Annotated[QueueForModel | ReindexEntity, Field(discriminator="command")]]


class Reindexer(ABC):
    root_config: RootConfig

    def __init__(self, root_config: RootConfig):
        self.root_config = root_config

    def start_reindex(self, specific_models: list[str] | None = None, batch_size: int = 100):
        model_names = specific_models or self.root_config.registered_models.keys()
        LOGGER.info("Starting reindex for the following models: %s", model_names)
        job_id = ulid.new().str
        payloads: list[str] = []
        for model_name in model_names:
            payloads.append(
                QueueForModel(
                    index_job_id=job_id,
                    command="queue_for_model",
                    entity_name=model_name,
                    last_evaluated_key="",
                    batch_size=batch_size,
                ).model_dump_json()
            )

    @abstractmethod
    def enqueuer(self, payloads: list[str]): ...

    def processor(self, payloads: list[str]):
        for payload in payloads:
            command = Command.model_validate_json(payload)
            if isinstance(command, QueueForModel):
                self.process_queue_for_model(command)
            elif isinstance(command, ReindexEntity):
                self.process_reindex_entity(command)
            else:
                LOGGER.error(f"Invalid command: {command}")

    def process_reindex_entity(self, command: ReindexEntity):
        model_cls = self.root_config.registered_models[command.entity_name]
        entity = model_cls.get_by_id(command.entity_id)
        if entity is None:
            LOGGER.error("Entity not found for reindexing: %s", command.entity_id)
            return

        entity.save()

    def process_queue_for_model(self, command: QueueForModel):
        model_cls = self.root_config.registered_models[command.entity_name]
        response = model_cls.query(
            AllQuery(),
            last_evaluated_key=json.loads(command.last_evaluated_key) if command.last_evaluated_key else None,
            max_records=command.batch_size,
            lazy=False,
        )
        records = list(response.records)
        new_commands: list[str] = []
        if response.last_evaluated_key:
            new_commands.append(
                QueueForModel(
                    index_job_id=command.index_job_id,
                    command="queue_for_model",
                    entity_name=command.entity_name,
                    last_evaluated_key=json.dumps(response.last_evaluated_key),
                    batch_size=command.batch_size,
                ).model_dump_json()
            )

        for record in records:
            new_commands.append(
                ReindexEntity(
                    index_job_id=command.index_job_id,
                    command="reindex_entity",
                    entity_name=command.entity_name,
                    entity_id=record.id,
                ).model_dump_json()
            )

        self.enqueuer(new_commands)

    def notify(self):
        raise NotImplementedError("Notify method not implemented.")
