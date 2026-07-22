"""Tests for API models."""


from salesforce_datacloud_connector.api.models import QueryStatus, ColumnMetadata, QueryResponse


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
