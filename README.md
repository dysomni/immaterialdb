# immaterialdb

A materialized, graph-based client for DynamoDB, providing a high-level, model-driven API for Python applications. Define Pydantic models with rich indexing, querying, and field-level encryption, all backed by DynamoDB.

---

## Features

- **Model-driven**: Define your data models as Pydantic classes.
- **Rich Indexing**: Unique and compound indices for efficient lookups and queries.
- **Advanced Querying**: Expressive, type-safe query API.
- **Field-level Encryption**: Register custom encryption/decryption for sensitive fields.
- **Testing Utilities**: Easily mock DynamoDB for tests.

---

## Installation

Add to your `pyproject.toml`:

```toml
[project]
dependencies = [
    "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.12",
    ...
]
```

Or install via pip:

```sh
pip install "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.12"
```

---

## DynamoDB Table Setup

immaterialdb requires a DynamoDB table with a specific schema and set of global secondary indexes (GSIs). You can provision this table using the provided Terraform module, or manually via the AWS Console/CLI.

### Using Terraform

A Terraform module is provided in `immaterialtf/` to create the required table and IAM policy. Example usage:

```hcl
module "immaterialdb_table" {
  source            = "./immaterialtf"
  table_name        = "my_table"
  delete_protection = true # or false
  tags = {
    Environment = "dev"
    Project     = "immaterialdb"
  }
}
```

This will create a DynamoDB table with the following attributes and GSIs:

- **Primary Key**: `pk` (string, HASH), `sk` (string, RANGE)
- **Attributes**: `entity_id` (string), `entity_name` (string), `base_node_id` (string)
- **GSI 1**: `ids_only` — HASH: `entity_id`, projection: ALL
- **GSI 2**: `model_scan` — HASH: `entity_name`, RANGE: `base_node_id`, projection: ALL

Outputs:

- `table_name`: The name of the table
- `table_arn`: The ARN of the table
- `iam_policy_arn`: The ARN of the IAM policy for DynamoDB access

### Manual Setup (AWS Console/CLI)

If not using Terraform, create a DynamoDB table with:

- **Table name**: (your choice, e.g. `my_table`)
- **Partition key**: `pk` (String)
- **Sort key**: `sk` (String)

Add the following attributes (all type String):

- `entity_id`
- `entity_name`
- `base_node_id`

Add these Global Secondary Indexes:

1. **ids_only**
   - Partition key: `entity_id` (String)
   - Projection: ALL
2. **model_scan**
   - Partition key: `entity_name` (String)
   - Sort key: `base_node_id` (String)
   - Projection: ALL

Make sure your application has IAM permissions for: `dynamodb:PutItem`, `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:DeleteItem`, `dynamodb:Query`, and `dynamodb:Scan` on the table.

---

## Quickstart

### 1. Setup

```python
from immaterialdb import RootConfig

IMMATERIALDB = RootConfig("my_table")
```

### 2. (Optional) Register Encryption

```python
@IMMATERIALDB.decorators.register_encryption
def encrypt_string(text: str) -> str:
    # Replace with real encryption
    return text[::-1]

@IMMATERIALDB.decorators.register_decryption
def decrypt_string(text: str) -> str:
    return text[::-1]
```

### 3. Define and Register a Model

```python
from immaterialdb import Model, Indices

@IMMATERIALDB.decorators.register_model(
    [
        Indices.Query(partition_fields=["username"], sort_fields=["created_at"]),
        Indices.Unique(unique_fields=["username"]),
    ],
    encrypted_fields=["secret_field"],
    auto_decrypt=True,
)
class User(Model):
    username: str
    email: str
    secret_field: str
```

### 4. Create, Save, and Retrieve Records

```python
user = User(username="alice", email="alice@example.com", secret_field="topsecret")
user.save()

fetched = User.get_by_id(user.id)
print(fetched.username)  # "alice"
print(fetched.secret_field)  # "topsecret" (decrypted automatically)
```

### 5. Querying

```python
from immaterialdb import Queries

results = User.query(Queries.Standard([Queries.Standard.Statement("username", "eq", "alice")]))
for user in results.records:
    print(user.email)
```

### 6. Deleting

```python
user.delete()
```

---

## Advanced Features

- **Batch Queries**: Use `.query()` with various query types for batch and paginated results.
- **Custom Indices**: Define multiple indices for flexible access patterns.
- **Testing Utilities**: Use `@mock_immaterialdb(IMMATERIALDB)` to mock DynamoDB in your tests.
- **Field-level Encryption**: Specify `encrypted_fields` and register your own encryption/decryption logic.

---

## Testing Example

```python
from immaterialdb.testing import mock_immaterialdb

@mock_immaterialdb(IMMATERIALDB)
def test_user_crud():
    user = User(username="alice", email="alice@example.com", secret_field="topsecret")
    user.save()
    assert User.get_by_id(user.id).username == "alice"
```

---

## License

MIT
