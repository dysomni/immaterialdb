from abc import ABC, abstractmethod
from enum import StrEnum, auto
from typing import Any, Literal, NamedTuple, Self

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from mypy_boto3_dynamodb.type_defs import AttributeValueTypeDef, TransactWriteItemTypeDef
from pydantic import BaseModel

from immaterialdb.types import FieldValue, PrimaryKeys
from immaterialdb.value_serializers import serialize_for_query_node_primary_key, serialize_for_unique_node_primary_key


class NodeTypes(StrEnum):
    base = auto()
    unique = auto()
    query = auto()


class Node(BaseModel, ABC):
    node_type: NodeTypes
    entity_name: str
    entity_id: str
    pk: str
    sk: str

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Node):
            return False

        return self.pk == other.pk and self.sk == other.sk

    def assemble_transaction_item_delete(self, table_name: str) -> TransactWriteItemTypeDef:
        return {
            "Delete": {
                "Key": {"pk": TypeSerializer().serialize(self.pk), "sk": TypeSerializer().serialize(self.sk)},
                "TableName": table_name,
            }
        }

    @abstractmethod
    def assemble_transaction_item_put(self, table_name: str) -> TransactWriteItemTypeDef:
        pass

    def for_dynamo(self) -> dict[str, AttributeValueTypeDef]:
        return {k: TypeSerializer().serialize(v) for k, v in self.model_dump().items()}

    @classmethod
    def from_dynamo(cls, dynamo_item: dict[str, AttributeValueTypeDef]) -> Self:
        return cls.model_validate({k: TypeDeserializer().deserialize(v) for k, v in dynamo_item.items()})


class BaseNode(Node):
    node_type: Literal[NodeTypes.base] = NodeTypes.base
    base_node_id: str
    raw_data: str
    other_nodes: PrimaryKeys

    def assemble_transaction_item_put(self, table_name: str) -> TransactWriteItemTypeDef:
        return {
            "Put": {
                "Item": self.for_dynamo(),
                "TableName": table_name,
            }
        }


class UniqueNode(Node):
    node_type: Literal[NodeTypes.unique] = NodeTypes.unique
    unique_node_id: str
    fields: list[FieldValue]

    @classmethod
    def create(cls, entity_name: str, entity_id: str, fields: list[FieldValue]) -> Self:
        pk, sk = serialize_for_unique_node_primary_key(entity_name, fields)
        return cls(entity_name=entity_name, entity_id=entity_id, fields=fields, pk=pk, sk=sk, unique_node_id=entity_id)

    def assemble_transaction_item_put(self, table_name: str) -> TransactWriteItemTypeDef:
        return {
            "Put": {
                "Item": self.for_dynamo(),
                "TableName": table_name,
                "ConditionExpression": "attribute_not_exists(pk) OR entity_id = :current_id",
                "ExpressionAttributeValues": {":current_id": TypeSerializer().serialize(self.entity_id)},
            }
        }


class QueryNode(Node):
    node_type: Literal[NodeTypes.query] = NodeTypes.query
    query_node_id: str
    partition_fields: list[FieldValue]
    sort_fields: list[FieldValue]
    raw_data: str

    @classmethod
    def create(
        cls,
        entity_name: str,
        entity_id: str,
        partition_fields: list[FieldValue],
        sort_fields: list[FieldValue],
        raw_data: str,
    ) -> Self:
        pk, sk = serialize_for_query_node_primary_key(entity_name, entity_id, partition_fields, sort_fields)
        return cls(
            entity_name=entity_name,
            entity_id=entity_id,
            partition_fields=partition_fields,
            sort_fields=sort_fields,
            pk=pk,
            sk=sk,
            raw_data=raw_data,
            query_node_id=entity_id,
        )

    def assemble_transaction_item_put(self, table_name: str) -> TransactWriteItemTypeDef:
        return {
            "Put": {
                "Item": self.for_dynamo(),
                "TableName": table_name,
            }
        }


NodeType = BaseNode | UniqueNode | QueryNode
NodeTypeList = list[NodeType]
NodeTransactionItem = NamedTuple("NodeTransactionItem", [("node", NodeType), ("action", Literal["put", "delete"])])
NodeTransactionList = list[NodeTransactionItem]
