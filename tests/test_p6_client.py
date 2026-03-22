"""Unit tests for src/p6_client.py — all HTTP calls mocked via responses library."""
import pytest
import responses as resp_mock
from requests import HTTPError

from src.p6_client import OPCClient

MOCK_CONFIG = {
    "base_url": "https://test-tenant.oraclecloud.com/ppmrestapi/v2",
    "token_url": "https://idcs-test.identity.oraclecloud.com/oauth2/v1/token",
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "scope": "",
    "timeout": 5,
    "max_retries": 1,
    "retry_wait": 0,
    "page_size": 100,
    "fields": {
        "project_fields": ["ObjectId", "Id", "Name", "Status"],
        "wbs_fields": ["ObjectId", "ProjectObjectId", "Code", "Name"],
        "activity_fields": ["ObjectId", "ProjectObjectId", "Id", "Name"],
        "resource_fields": ["ObjectId", "Id", "Name", "ResourceType"],
        "resource_assignment_fields": ["ObjectId", "ProjectObjectId", "ResourceObjectId", "PlannedUnits", "ActualUnits"],
    },
}

TOKEN_RESPONSE = {
    "access_token": "mock-access-token",
    "token_type": "Bearer",
    "expires_in": 3600,
}


@pytest.fixture
def opc_client():
    with resp_mock.RequestsMock() as rsps:
        rsps.add(
            resp_mock.POST,
            MOCK_CONFIG["token_url"],
            json=TOKEN_RESPONSE,
            status=200,
        )
        client = OPCClient(config=MOCK_CONFIG)
        yield client, rsps


class TestOPCClientProjects:

    @resp_mock.activate
    def test_get_projects_returns_list(self):
        resp_mock.add(
            resp_mock.POST,
            MOCK_CONFIG["token_url"],
            json=TOKEN_RESPONSE,
            status=200,
        )
        projects = [
            {"ObjectId": 1, "Id": "PRJ-001", "Name": "Project Alpha", "Status": "Active"},
            {"ObjectId": 2, "Id": "PRJ-002", "Name": "Project Beta", "Status": "Active"},
        ]
        resp_mock.add(
            resp_mock.GET,
            f"{MOCK_CONFIG['base_url']}/projects",
            json=projects,
            status=200,
        )
        client = OPCClient(config=MOCK_CONFIG)
        result = client.get_projects()
        assert len(result) == 2
        assert result[0]["ObjectId"] == 1

    @resp_mock.activate
    def test_get_projects_with_status_filter(self):
        resp_mock.add(
            resp_mock.POST,
            MOCK_CONFIG["token_url"],
            json=TOKEN_RESPONSE,
            status=200,
        )
        resp_mock.add(
            resp_mock.GET,
            f"{MOCK_CONFIG['base_url']}/projects",
            json=[{"ObjectId": 3, "Id": "PRJ-003", "Name": "What-if Project", "Status": "What-if"}],
            status=200,
        )
        client = OPCClient(config=MOCK_CONFIG)
        result = client.get_projects(status_filter=["What-if"])
        assert len(result) == 1
        assert result[0]["Status"] == "What-if"

    @resp_mock.activate
    def test_get_projects_http_error_raises(self):
        resp_mock.add(
            resp_mock.POST,
            MOCK_CONFIG["token_url"],
            json=TOKEN_RESPONSE,
            status=200,
        )
        resp_mock.add(
            resp_mock.GET,
            f"{MOCK_CONFIG['base_url']}/projects",
            json={"error": "Unauthorized"},
            status=401,
        )
        client = OPCClient(config=MOCK_CONFIG)
        with pytest.raises(HTTPError):
            client.get_projects()

    @resp_mock.activate
    def test_pagination_collects_all_pages(self):
        resp_mock.add(
            resp_mock.POST,
            MOCK_CONFIG["token_url"],
            json=TOKEN_RESPONSE,
            status=200,
        )
        # First page: full page (page_size=100 items)
        page1 = [{"ObjectId": i, "Id": f"P{i}", "Name": f"Project {i}", "Status": "Active"} for i in range(100)]
        page2 = [{"ObjectId": 100, "Id": "P100", "Name": "Project 100", "Status": "Active"}]
        resp_mock.add(resp_mock.GET, f"{MOCK_CONFIG['base_url']}/projects", json=page1, status=200)
        resp_mock.add(resp_mock.GET, f"{MOCK_CONFIG['base_url']}/projects", json=page2, status=200)

        client = OPCClient(config=MOCK_CONFIG)
        result = client.get_projects()
        assert len(result) == 101


class TestOPCClientWBS:

    @resp_mock.activate
    def test_get_wbs_filters_by_project(self):
        resp_mock.add(resp_mock.POST, MOCK_CONFIG["token_url"], json=TOKEN_RESPONSE, status=200)
        resp_mock.add(
            resp_mock.GET,
            f"{MOCK_CONFIG['base_url']}/wbs",
            json=[{"ObjectId": 10, "ProjectObjectId": 1, "Code": "1.0", "Name": "Phase 1"}],
            status=200,
        )
        client = OPCClient(config=MOCK_CONFIG)
        result = client.get_wbs(project_object_id=1)
        assert len(result) == 1
        assert result[0]["ProjectObjectId"] == 1


class TestOPCClientResources:

    @resp_mock.activate
    def test_get_resources_returns_all(self):
        resp_mock.add(resp_mock.POST, MOCK_CONFIG["token_url"], json=TOKEN_RESPONSE, status=200)
        resp_mock.add(
            resp_mock.GET,
            f"{MOCK_CONFIG['base_url']}/resources",
            json=[
                {"ObjectId": 1, "Id": "RES-001", "Name": "Alice", "ResourceType": "Labor"},
                {"ObjectId": 2, "Id": "RES-002", "Name": "Bob", "ResourceType": "Labor"},
            ],
            status=200,
        )
        client = OPCClient(config=MOCK_CONFIG)
        result = client.get_resources()
        assert len(result) == 2
