import requests_mock
import pytest

from bubble_api import BubbleWrapper


BASE_URL_EXAMPLE = "https://example.com"
API_URL_EXAMPLE = f"{BASE_URL_EXAMPLE}/version-test/api/1.1/obj"


@pytest.fixture(scope="session")
def bubble_wrapper():
    return BubbleWrapper(
        base_url=BASE_URL_EXAMPLE,
        api_key="API_KEY",
        bubble_version="test",
    )


@pytest.fixture
def mocker():
    with requests_mock.Mocker() as mocker:
        yield mocker


def test__bubble_get_by_id(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(expected_url, json={"response": {"data": "test"}})

    bubble_wrapper.get_by_id(example_table, example_id)

    assert mocker.called_once
    assert mocker.last_request.method == "GET"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.body is None


def test__bubble_create_object(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    example_object = {"field_1": 0, "field_2": 4.5}
    expected_url = f"{API_URL_EXAMPLE}/{example_table}"

    mocker.post(expected_url, json={"id": example_id})

    object_id = bubble_wrapper.create(
        example_table,
        example_object,
    )

    assert object_id == example_id
    assert mocker.called_once
    assert mocker.last_request.method == "POST"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.json() == example_object


def test__delete_object(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"

    mocker.delete(expected_url)

    bubble_wrapper.delete_by_id(
        example_table,
        example_id,
    )

    assert mocker.called_once
    assert mocker.last_request.method == "DELETE"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.body is None


def test__bubble_update_object(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    example_fields = {"field_1": 0, "field_2": 4.5}
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"

    mocker.patch(expected_url)

    bubble_wrapper.update_object(
        example_table,
        example_id,
        example_fields,
    )

    assert mocker.called_once
    assert mocker.last_request.method == "PATCH"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.json() == example_fields


def test__bubble_replace_object(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    example_fields = {"field_1": 0, "field_2": 4.5}
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"

    mocker.put(expected_url)

    bubble_wrapper.replace_object(
        example_table,
        example_id,
        example_fields,
    )

    assert mocker.called_once
    assert mocker.last_request.method == "PUT"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.json() == example_fields
