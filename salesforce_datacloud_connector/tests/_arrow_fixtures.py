"""
Test-only helper for building Arrow IPC stream bytes.

pyarrow is used here purely as a fixture-building tool to serialize real Arrow
IPC bytes for tests to feed into the connector's nanoarrow-based parsing code
(api/models.py). pyarrow is a dev-only dependency; the connector itself parses
Arrow with nanoarrow, never pyarrow.
"""

import io
from typing import Any, List, Sequence, Tuple

import nanoarrow as na
import pyarrow as pa


def build_arrow_ipc_bytes(fields: Sequence[pa.Field], rows: List[Tuple[Any, ...]]) -> bytes:
    """
    Serialize rows into an Arrow IPC stream matching the given pyarrow schema fields.

    Args:
        fields: pyarrow Field objects defining column name/type/nullability
        rows: Row tuples, one value per field, in field order (may be empty)

    Returns:
        Raw Arrow IPC stream bytes, as the v3 API would return them in the
        response body when Accept negotiates application/vnd.apache.arrow.stream
    """
    schema = pa.schema(fields)
    columns = list(zip(*rows)) if rows else [[] for _ in fields]
    arrays = [pa.array(column, type=field.type) for column, field in zip(columns, fields)]

    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as writer:
        if rows:
            writer.write_batch(pa.record_batch(arrays, schema=schema))
    return sink.getvalue()


def nanoarrow_field_for(field: pa.Field) -> "na.Schema":
    """
    Build a genuine nanoarrow per-column Schema object for a single pyarrow
    field, by round-tripping it through a real Arrow IPC stream.

    ColumnMetadata.from_arrow_field() expects the nanoarrow Schema objects
    that come from a parsed IPC stream (na.ArrayStream(...).schema.fields),
    not raw pyarrow Field objects — their .type comparisons against
    na.Type.* are not interchangeable. Tests that want to exercise
    from_arrow_field() with a specific Arrow type use this helper rather
    than constructing a pyarrow Field directly.
    """
    raw = build_arrow_ipc_bytes([field], [])
    with na.ArrayStream.from_readable(raw) as stream:
        schema = stream.schema
        stream.read_all()
    return schema.fields[0]
