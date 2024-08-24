from enum import StrEnum, auto
from typing import Literal, Self

from pydantic import BaseModel

from immaterialdb.types import FieldValue, PrimaryKeys
from immaterialdb.value_serializers import serialize_for_query_node_primary_key, serialize_for_unique_node_primary_key


class NodeTypes(StrEnum):
    base = auto()
    unique = auto()
    query = auto()


class Node(BaseModel):
    node_type: NodeTypes
    model_name: str
    entity_id: str
    pk: str
    sk: str


class BaseNode(Node):
    node_type: Literal[NodeTypes.base] = NodeTypes.base
    base_node: Literal[NodeTypes.base] = NodeTypes.base
    raw_data: str
    other_nodes: PrimaryKeys


class UniqueNode(Node):
    node_type: Literal[NodeTypes.unique] = NodeTypes.unique
    unique_node: Literal[NodeTypes.unique] = NodeTypes.unique
    fields: list[FieldValue]

    @classmethod
    def create(cls, model_name: str, entity_id: str, fields: list[FieldValue]) -> Self:
        pk, sk = serialize_for_unique_node_primary_key(model_name, fields)
        return cls(model_name=model_name, entity_id=entity_id, fields=fields, pk=pk, sk=sk)


class QueryNode(Node):
    node_type: Literal[NodeTypes.query] = NodeTypes.query
    query_node: Literal[NodeTypes.query] = NodeTypes.query
    partition_fields: list[FieldValue]
    sort_fields: list[FieldValue]

    @classmethod
    def create(
        cls, model_name: str, entity_id: str, partition_fields: list[FieldValue], sort_fields: list[FieldValue]
    ) -> Self:
        pk, sk = serialize_for_query_node_primary_key(model_name, partition_fields, sort_fields)
        return cls(
            model_name=model_name,
            entity_id=entity_id,
            partition_fields=partition_fields,
            sort_fields=sort_fields,
            pk=pk,
            sk=sk,
        )


NodeType = BaseNode | UniqueNode | QueryNode
NodeTypeList = list[NodeType]
