"""
Data models for Salesforce Data Cloud Query API responses.

These models represent the structure of API responses from the Query API endpoints.
"""

from dataclasses import dataclass
from typing import Any, List, Optional


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
            data: Status dictionary from API response

        Returns:
            QueryStatus instance
        """
        return cls(
            query_id=data.get("queryId", ""),
            completion_status=data.get("completionStatus", ""),
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
            data: Response dictionary from API

        Returns:
            QueryResponse instance
        """
        # Parse metadata
        metadata = [
            ColumnMetadata.from_dict(col)
            for col in data.get("metadata", [])
        ]

        # Parse status if present
        status = None
        if "status" in data:
            status = QueryStatus.from_dict(data["status"])

        return cls(
            data=data.get("data", []),
            metadata=metadata,
            returned_rows=data.get("returnedRows", 0),
            status=status,
        )


@dataclass
class SqlParameter:
    """
    SQL parameter for parameterized queries.

    Attributes:
        name: Parameter name (without colon prefix)
        value: Parameter value
        type: Data Cloud type name
    """
    name: str
    value: Any
    type: str

    def to_dict(self) -> dict:
        """
        Convert to API request dictionary format.

        Returns:
            Dictionary with name, value, and type
        """
        return {
            "name": self.name,
            "value": self.value,
            "type": self.type,
        }
