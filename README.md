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
    "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.17",
    ...
]
```

Or install via pip:

```sh
pip install "immaterialdb @ git+https://github.com/dysomni/immaterialdb.git@v0.1.17"
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
- **Counters**: Add atomic, distributed counters to your models for safe concurrent increments/decrements. See the Counters section below.
- **Distributed Locking**: Use `record_lock` on models or manual locks via `IMMATERIALDB.dynamodb_provider.lock` for safe, distributed critical sections and to prevent race conditions. See the Distributed Locking section below.

---

## Counters

immaterialdb supports atomic, distributed counters for integer fields on your models. These are useful for fields like view counts, likes, or any value that needs to be incremented/decremented safely across concurrent processes.

### Declaring a Counter Field

Add the `counter_fields` argument to your model registration:

```python
@IMMATERIALDB.decorators.register_model(counter_fields=["age"])
class MyModelWithCounter(Model):
    age: int
```

### Using Counters

- **Increment/Decrement:**
  ```python
  record.increment_counter("age")         # age is now incremented by 1
  record.increment_counter("age", 10)     # age is incremented by 10
  record.increment_counter("age", -1)     # age is decremented by 1
  ```
- **Refresh from DB:**

  ```python
  record.refresh_counters()               # Syncs local value with DynamoDB
  ```

- **Atomicity:**
  All counter operations are atomic and safe for concurrent use across processes.

### Example

```python
record = MyModelWithCounter(age=30)
record.save()

record.increment_counter("age")         # age is now 31
record.increment_counter("age", 10)     # age is now 41

# In another process/thread:
other = MyModelWithCounter.get_by_id(record.id)
other.increment_counter("age")          # age is now 42

# Sync local value with DB
record.refresh_counters()               # record.age is now 42

# Decrement
record.increment_counter("age", -1)     # age is now 41
```

### API

- `increment_counter(field_name: str, amount: int = 1) -> int`: Atomically increments (or decrements) the counter and updates the model instance. Returns the new value.
- `refresh_counters()`: Fetches the latest counter values from DynamoDB and updates the model instance.

---

## Distributed Locking

immaterialdb provides distributed locking primitives to help you safely coordinate updates to records or arbitrary resources across multiple processes or machines.

### Record Locks

Each model instance provides a `record_lock` context manager. This ensures that only one process/thread can update a record at a time, preventing race conditions and concurrent modifications.

- **Usage:**

  ```python
  with record.record_lock(ttl=10, wait=2):
      # Safe to update this record here
      record.age += 1
      record.save()
  ```

  - `ttl`: How long (in seconds) the lock is held before it expires (default: 15).
  - `wait`: How long (in seconds) to wait for the lock before raising an error (default: 5).
  - If the lock cannot be acquired within `wait` seconds, a `LockNotAcquiredError` is raised.

### Manual Locks (Arbitrary Keys)

You can also acquire a distributed lock on any arbitrary key using the DynamoDB provider directly. This is useful for custom critical sections, distributed tasks, or coordination outside of model records.

- **Usage:**
  ```python
  with IMMATERIALDB.dynamodb_provider.lock("my-custom-lock", ttl=10, wait=2):
      # critical section
      do_something()
  ```
  - The semantics are the same as `record_lock`.
  - The lock key can be any string.

### Implementation Notes

- Locks are implemented using DynamoDB items with a special key and an expiration time.
- If a lock is already held and not expired, attempts to acquire will retry until `wait` seconds elapse.
- If the lock cannot be acquired, a `LockNotAcquiredError` is raised.
- Locks are released when the context exits, but will also expire after `ttl` seconds as a safety net.

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
