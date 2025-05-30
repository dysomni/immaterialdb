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
    "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.11",
    ...
]
```

Or install via pip:

```sh
pip install "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.11"
```

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
