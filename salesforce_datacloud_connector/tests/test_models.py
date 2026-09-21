"""Tests for API models."""

import datetime
from decimal import Decimal

import pyarrow as pa

from salesforce_datacloud_connector.api.models import QueryStatus, ColumnMetadata, QueryResponse

from ._arrow_fixtures import build_arrow_ipc_bytes, nanoarrow_field_for


def test_query_status_from_dict_normalizes_running_or_unspecified():
    """Test that RUNNING_OR_UNSPECIFIED is normalized to RUNNING."""
    data = {
        "queryId": "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab",
        "completionStatus": "RUNNING_OR_UNSPECIFIED",
        "progress": 0.5,
        "rowCount": 0,
        "chunkCount": 0,
        "expirationTime": "2025-09-26T10:55:07.438Z",
        "executionStats": {
            "wallClockTime": 1.122854338,
            "rowsProcessed": 5
        }
    }

    status = QueryStatus.from_dict(data)

    assert status.completion_status == "RUNNING"
    assert status.query_id == "MTAuMjQuMTIzLjE5NDo3NDg0_cb6991ab"
    assert status.progress == 0.5
    assert not status.is_complete()


def test_query_status_recognizes_results_produced():
    """Test that RESULTS_PRODUCED is recognized as complete."""
    data = {
        "queryId": "q123",
        "completionStatus": "RESULTS_PRODUCED",
        "progress": 1.0,
        "rowCount": 5,
        "chunkCount": 1,
    }

    status = QueryStatus.from_dict(data)

    assert status.is_complete()
    assert not status.is_running()


def test_query_status_recognizes_finished():
    """Test that FINISHED is recognized as complete."""
    data = {
        "queryId": "q456",
        "completionStatus": "FINISHED",
        "progress": 1.0,
        "rowCount": 10,
        "chunkCount": 1,
    }

    status = QueryStatus.from_dict(data)

    assert status.is_complete()


def test_column_metadata_from_dict_lowercase_types():
    """Test that lowercase type names are preserved."""
    data = {
        "name": "id__c",
        "type": "varchar",
        "nullable": False
    }

    col = ColumnMetadata.from_dict(data)

    assert col.name == "id__c"
    assert col.type == "varchar"
    assert col.nullable is False


def test_query_response_from_dict_v3_shape():
    """Test parsing v3 response shape with metadata.columns wrapper."""
    data = {
        "metadata": {
            "columns": [
                {"name": "id__c", "type": "varchar", "nullable": False},
                {"name": "name__c", "type": "varchar", "nullable": True}
            ]
        },
        "data": [
            ["c1ce2a48-9d03-4eed-938d-f354db79c425", "Gowtham"]
        ],
        "returnedRows": 1
    }

    response = QueryResponse.from_dict(data)

    assert len(response.metadata) == 2
    assert response.metadata[0].name == "id__c"
    assert response.metadata[0].type == "varchar"
    assert len(response.data) == 1
    assert response.data[0] == ["c1ce2a48-9d03-4eed-938d-f354db79c425", "Gowtham"]
    assert response.returned_rows == 1


# --- Arrow output format (v3 Accept: application/vnd.apache.arrow.stream) ---


def test_column_metadata_from_arrow_field_varchar():
    """Test that a nullable Arrow string field maps to varchar."""
    field = nanoarrow_field_for(pa.field("name", pa.string(), nullable=True))
    col = ColumnMetadata.from_arrow_field(field)

    assert col.name == "name"
    assert col.type == "varchar"
    assert col.nullable is True
    assert col.precision is None
    assert col.scale is None


def test_column_metadata_from_arrow_field_decimal_carries_precision_scale():
    """Test that decimal128 maps to numeric with precision/scale preserved."""
    field = nanoarrow_field_for(pa.field("amount", pa.decimal128(10, 3), nullable=False))
    col = ColumnMetadata.from_arrow_field(field)

    assert col.type == "numeric"
    assert col.nullable is False
    assert col.precision == 10
    assert col.scale == 3


def test_column_metadata_from_arrow_field_int64_is_bigint():
    """Test that int64 maps to bigint (not integer)."""
    field = nanoarrow_field_for(pa.field("id", pa.int64(), nullable=False))
    col = ColumnMetadata.from_arrow_field(field)
    assert col.type == "bigint"


def test_column_metadata_from_arrow_field_int32_is_integer():
    """Test that int32 maps to integer."""
    field = nanoarrow_field_for(pa.field("count", pa.int32(), nullable=True))
    col = ColumnMetadata.from_arrow_field(field)
    assert col.type == "integer"


def test_column_metadata_from_arrow_field_boolean():
    """Test that bool maps to boolean."""
    field = nanoarrow_field_for(pa.field("active", pa.bool_(), nullable=True))
    col = ColumnMetadata.from_arrow_field(field)
    assert col.type == "boolean"


def test_column_metadata_from_arrow_field_date32():
    """Test that date32 maps to date."""
    field = nanoarrow_field_for(pa.field("birthday", pa.date32(), nullable=True))
    col = ColumnMetadata.from_arrow_field(field)
    assert col.type == "date"


def test_column_metadata_from_arrow_field_timestamp_tz():
    """Test that a tz-aware timestamp maps to timestamptz."""
    field = nanoarrow_field_for(pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=True))
    col = ColumnMetadata.from_arrow_field(field)
    assert col.type == "timestamptz"


def test_column_metadata_from_arrow_field_float_and_double():
    """Test that float32/float64 map to float/double respectively."""
    float_col = ColumnMetadata.from_arrow_field(nanoarrow_field_for(pa.field("f", pa.float32(), nullable=True)))
    double_col = ColumnMetadata.from_arrow_field(nanoarrow_field_for(pa.field("d", pa.float64(), nullable=True)))
    assert float_col.type == "float"
    assert double_col.type == "double"


def test_query_response_from_arrow_bytes_basic_round_trip():
    """Test that an Arrow IPC stream round-trips to the same shape as from_dict()."""
    fields = [
        pa.field("name", pa.string(), nullable=True),
        pa.field("age", pa.decimal128(10, 0), nullable=False),
    ]
    raw = build_arrow_ipc_bytes(fields, [("Alice", 30), ("Bob", 25)])

    response = QueryResponse.from_arrow_bytes(raw)

    assert len(response.metadata) == 2
    assert response.metadata[0].name == "name"
    assert response.metadata[0].type == "varchar"
    assert response.metadata[1].name == "age"
    assert response.metadata[1].type == "numeric"
    assert response.data == [["Alice", Decimal("30")], ["Bob", Decimal("25")]]
    assert response.returned_rows == 2
    assert response.status is None


def test_query_response_from_arrow_bytes_preserves_native_python_types():
    """Test that nanoarrow produces native Python types for each SQL type category."""
    fields = [
        pa.field("amount", pa.decimal128(10, 3), nullable=True),
        pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("d", pa.date32(), nullable=True),
        pa.field("flag", pa.bool_(), nullable=True),
    ]
    ts = datetime.datetime(2025, 9, 26, 10, 55, 7, tzinfo=datetime.timezone.utc)
    d = datetime.date(2025, 9, 26)
    raw = build_arrow_ipc_bytes(fields, [(Decimal("123.456"), ts, d, True)])

    response = QueryResponse.from_arrow_bytes(raw)
    row = response.data[0]

    assert row[0] == Decimal("123.456")
    assert row[1] == ts
    assert row[2] == d
    assert row[3] is True


def test_query_response_from_arrow_bytes_nulls_become_none():
    """Test that NULL Arrow values convert to Python None regardless of column type."""
    fields = [
        pa.field("name", pa.string(), nullable=True),
        pa.field("age", pa.int64(), nullable=True),
    ]
    raw = build_arrow_ipc_bytes(fields, [(None, None)])

    response = QueryResponse.from_arrow_bytes(raw)

    assert response.data == [[None, None]]


def test_query_response_from_arrow_bytes_empty_result_keeps_schema():
    """Test that a zero-row IPC stream still yields metadata from the schema message."""
    fields = [pa.field("name", pa.string(), nullable=True)]
    raw = build_arrow_ipc_bytes(fields, [])

    response = QueryResponse.from_arrow_bytes(raw)

    assert response.data == []
    assert response.returned_rows == 0
    assert len(response.metadata) == 1
    assert response.metadata[0].name == "name"
    assert response.metadata[0].type == "varchar"


def test_query_response_from_arrow_bytes_attaches_supplied_status():
    """Test that a status object passed in by the caller (parsed from the
    x-hyperdb-status header) is attached to the Arrow-parsed response."""
    fields = [pa.field("name", pa.string(), nullable=True)]
    raw = build_arrow_ipc_bytes(fields, [("Alice",)])
    status = QueryStatus.from_dict({"queryId": "q1", "completionStatus": "FINISHED",
                                     "progress": 1.0, "rowCount": 1, "chunkCount": 1})

    response = QueryResponse.from_arrow_bytes(raw, status=status)

    assert response.status is status
    assert response.status.query_id == "q1"
