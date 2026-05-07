"""
Table metadata structures for Salesforce Data Cloud schema introspection.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Field:
    """
    Column/field metadata for a Data Cloud table.

    Attributes:
        name: Field name
        display_name: Human-readable field name
        type: Data Cloud type (Varchar, Numeric, TimestampTZ, etc.)
        nullable: Whether field can contain NULL
        precision: Numeric precision (for Numeric types)
        scale: Numeric scale (for Numeric types)
    """

    name: str
    display_name: str
    type: str
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Field":
        """Create Field from API response dictionary."""
        return cls(
            name=data.get("name", ""),
            display_name=data.get("displayName", data.get("name", "")),
            type=data.get("type", "Varchar"),
            nullable=data.get("nullable", True),
            precision=data.get("precision"),
            scale=data.get("scale"),
        )


@dataclass
class DataCloudTable:
    """
    Complete metadata for a Data Cloud table.

    Attributes:
        name: Table name
        display_name: Human-readable table name
        category: Schema name
        dataspace: Dataspace name
        fields: List of field metadata
    """

    name: str
    display_name: str
    category: str
    dataspace: str
    fields: List[Field]

    @classmethod
    def from_dict(cls, data: dict, dataspace: str) -> "DataCloudTable":
        """Create DataCloudTable from API response dictionary."""
        fields = [Field.from_dict(field_data) for field_data in data.get("fields", [])]

        return cls(
            name=data.get("name", ""),
            display_name=data.get("displayName", data.get("name", "")),
            category=data.get("category", ""),
            dataspace=dataspace,
            fields=fields,
        )

    def get_field(self, field_name: str) -> Optional[Field]:
        """Get field metadata by name."""
        for field in self.fields:
            if field.name == field_name:
                return field
        return None
