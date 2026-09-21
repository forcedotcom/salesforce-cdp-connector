"""
Data models for Salesforce Data Cloud Query API responses.

These models represent the structure of API responses from the Query API endpoints.
"""

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import nanoarrow as na


@dataclass
class QueryStatus:
    """
    Status information for a query.

    Attributes:
        query_id: Unique identifier for the query
        completion_status: Status of query execution (Running, ResultsProduced, Finished, etc.)
        progress: Progress percentage (0.0 to 1.0)
        row_count: Total number of rows in the result set
        chunk_count: Number of chunks the results are divided into
        expiration_time: When the query results expire (if applicable)
    """
    query_id: str
    completion_status: str
    progress: float
    row_count: int
    chunk_count: int
    expiration_time: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "QueryStatus":
        """
        Create QueryStatus from API response dictionary.

        Args:
            data: Status dictionary from API response (v3: from x-hyperdb-status header)

        Returns:
            QueryStatus instance
        """
        completion_status = data.get("completionStatus", "")

        # Normalize v3 enum: RUNNING_OR_UNSPECIFIED → RUNNING
        if completion_status.upper() == "RUNNING_OR_UNSPECIFIED":
            completion_status = "RUNNING"

        return cls(
            query_id=data.get("queryId", ""),
            completion_status=completion_status,
            progress=data.get("progress", 0.0),
            row_count=data.get("rowCount", 0),
            chunk_count=data.get("chunkCount", 0),
            expiration_time=data.get("expirationTime"),
        )

    def is_complete(self) -> bool:
        """
        Check if the query has completed execution.

        The completion status is case-insensitive and may use different formats:
        - "ResultsProduced" or "RESULTSPRODUCED" or "RESULTS_PRODUCED"
        - "Finished" or "FINISHED"

        Returns:
            True if query is complete, False otherwise
        """
        status_upper = self.completion_status.upper().replace("_", "")
        return status_upper in ("RESULTSPRODUCED", "FINISHED")

    def is_running(self) -> bool:
        """
        Check if the query is still running.

        Returns:
            True if query is running, False otherwise
        """
        return not self.is_complete()


def _arrow_type_to_datacloud_type(field: "na.Schema") -> Tuple[str, Optional[int], Optional[int]]:
    """
    Map a nanoarrow field's Arrow type to the lowercase Data Cloud type
    vocabulary already recognized by types.py (varchar/numeric/etc.), plus
    precision/scale for decimal columns.

    Args:
        field: Per-column Schema object (from an IPC stream's struct schema)

    Returns:
        (datacloud_type_name, precision, scale)
    """
    arrow_type = field.type
    precision: Optional[int] = None
    scale: Optional[int] = None

    if arrow_type == na.Type.BOOL:
        type_name = "boolean"
    elif arrow_type in (
        na.Type.INT8, na.Type.INT16, na.Type.INT32,
        na.Type.UINT8, na.Type.UINT16, na.Type.UINT32,
    ):
        type_name = "integer"
    elif arrow_type in (na.Type.INT64, na.Type.UINT64):
        type_name = "bigint"
    elif arrow_type in (na.Type.FLOAT, na.Type.HALF_FLOAT):
        type_name = "float"
    elif arrow_type == na.Type.DOUBLE:
        type_name = "double"
    elif arrow_type in (na.Type.DECIMAL128, na.Type.DECIMAL256):
        type_name = "numeric"
        precision = field.precision
        scale = field.scale
    elif arrow_type in (na.Type.STRING, na.Type.LARGE_STRING, na.Type.STRING_VIEW):
        type_name = "varchar"
    elif arrow_type in (na.Type.DATE32, na.Type.DATE64):
        type_name = "date"
    elif arrow_type == na.Type.TIMESTAMP:
        type_name = "timestamptz"
    else:
        # Unrecognized Arrow type: fall back to varchar, matching the
        # existing ColumnMetadata.from_dict() default for unknown types.
        type_name = "varchar"

    return type_name, precision, scale


@dataclass
class ColumnMetadata:
    """
    Metadata for a result column.

    Attributes:
        name: Column name
        type: Data Cloud type name (e.g., "Varchar", "Numeric", "TimestampTZ")
        nullable: Whether the column can contain NULL values
        precision: Numeric precision (for Numeric types)
        scale: Numeric scale (for Numeric types)
    """
    name: str
    type: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "ColumnMetadata":
        """
        Create ColumnMetadata from API response dictionary.

        Args:
            data: Metadata dictionary from API response

        Returns:
            ColumnMetadata instance
        """
        return cls(
            name=data.get("name", ""),
            type=data.get("type", "Varchar"),
            nullable=data.get("nullable", True),
            precision=data.get("precision"),
            scale=data.get("scale"),
        )

    @classmethod
    def from_arrow_field(cls, field: "na.Schema") -> "ColumnMetadata":
        """
        Create ColumnMetadata from a nanoarrow field schema (v3 Arrow output format).

        Args:
            field: Per-column Schema object from the IPC stream's struct schema

        Returns:
            ColumnMetadata instance
        """
        type_name, precision, scale = _arrow_type_to_datacloud_type(field)
        return cls(
            name=field.name,
            type=type_name,
            nullable=field.nullable,
            precision=precision,
            scale=scale,
        )


@dataclass
class QueryResponse:
    """
    Complete response from a query execution or result fetch.

    Attributes:
        data: List of rows, where each row is a list of values
        metadata: List of column metadata
        returned_rows: Number of rows returned in this response
        status: Query status information (optional, not present in all responses)
    """
    data: List[List[Any]]
    metadata: List[ColumnMetadata]
    returned_rows: int
    status: Optional[QueryStatus] = None

    @classmethod
    def from_dict(cls, data: dict) -> "QueryResponse":
        """
        Create QueryResponse from API response dictionary.

        Args:
            data: Response dictionary from API (v3: metadata.columns wrapper)

        Returns:
            QueryResponse instance
        """
        # V3 wraps columns in metadata.columns, v2 had flat metadata array
        metadata_raw = data.get("metadata", {})
        if isinstance(metadata_raw, dict):
            # V3 shape: {"metadata": {"columns": [...]}}
            columns = metadata_raw.get("columns", [])
        else:
            # V2 shape: {"metadata": [...]}
            columns = metadata_raw

        # Parse metadata
        metadata = [ColumnMetadata.from_dict(col) for col in columns]

        # Parse status if present (not in fetch_results responses)
        status = None
        if "status" in data:
            status = QueryStatus.from_dict(data["status"])

        return cls(
            data=data.get("data", []),
            metadata=metadata,
            returned_rows=data.get("returnedRows", 0),
            status=status,
        )

    @classmethod
    def from_arrow_bytes(cls, raw: bytes, status: Optional[QueryStatus] = None) -> "QueryResponse":
        """
        Create QueryResponse by parsing an Arrow IPC stream (v3 Arrow output format).

        The v3 API returns Arrow IPC stream bytes directly as the response body
        when the request's Accept header negotiates
        application/vnd.apache.arrow.stream. Unlike the JSON body, this stream
        never carries status — the caller parses that separately from the
        x-hyperdb-status response header, as it does for the JSON path.

        Args:
            raw: Raw Arrow IPC stream bytes (response.content)
            status: Optional QueryStatus to attach (parsed by the caller from
                the x-hyperdb-status header)

        Returns:
            QueryResponse instance
        """
        with na.ArrayStream.from_readable(raw) as stream:
            schema = stream.schema
            array = stream.read_all()

        metadata = [ColumnMetadata.from_arrow_field(field) for field in schema.fields]
        rows = [list(row) for row in array.iter_tuples()]

        return cls(
            data=rows,
            metadata=metadata,
            returned_rows=len(rows),
            status=status,
        )
