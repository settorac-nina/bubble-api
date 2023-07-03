import json
from time import perf_counter
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import requests_mock

from bubble_api import BubbleWrapper, Field

BASE_URL_EXAMPLE = "https://example.com"
OBJ_API_URL_EXAMPLE = f"{BASE_URL_EXAMPLE}/version-test/api/1.1/obj"

DELTA_TIME_ERROR = 0.01


def extract_url_params(url):
    parse_result = urlparse(url)
    return parse_qs(str(parse_result.query))


@pytest.fixture(scope="session")
def bubble_wrapper():
    return BubbleWrapper(
        base_url=BASE_URL_EXAMPLE,
        api_token="API_KEY",
        bubble_version="test",
    )


@pytest.fixture
def mocker():
    with requests_mock.Mocker() as mocker:
        yield mocker


def test__bubble_get_by_id(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(expected_url, json={"response": {"_id": "123x123"}})

    bubble_wrapper.get_by_id(example_table, example_id)

    assert mocker.called_once
    assert mocker.last_request.method == "GET"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.body is None


def test__bubble_create_object(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    example_object = {"field_1": 0, "field_2": 4.5}
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}"

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


def test__delete_object_by_id(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"

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
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"

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
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"

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


def test__bubble_count_objects(bubble_wrapper, mocker):
    example_table = "example_table"
    constraints = [
        Field("Name") == "Bob",
        Field("Age") > 18,
    ]
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}"

    mocker.get(
        expected_url,
        json={
            "response": {
                "results": [{"_id": "123x123"}],
                "count": 1,
                "remaining": 149,
                "cursor": 1
            }
        },
    )

    count = bubble_wrapper.count_objects(example_table, constraints)

    assert count == 150
    assert mocker.called_once
    assert mocker.last_request.method == "GET"
    assert mocker.last_request.url.startswith(expected_url)

    last_request_params = extract_url_params(mocker.last_request.url)
    assert [con.to_dict() for con in constraints] == json.loads(
        last_request_params["constraints"][0]
    )
    assert json.loads(last_request_params["limit"][0]) == 1
    assert json.loads(last_request_params["cursor"][0]) == 0
    assert mocker.last_request.body is None


def test__bubble_retry_server_error(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(
        expected_url,
        [
            {"text": "Server Error", "status_code": 500},
            {"json": {"response": {"_id": "123x123"}}, "status_code": 200},
        ],
    )

    bubble_wrapper.get_by_id(example_table, example_id)

    assert mocker.call_count == 2
    assert mocker.last_request.method == "GET"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.body is None


def test__bubble_retry_correct_number_of_time(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(
        expected_url,
        text="Server Error",
        status_code=500,
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_wrapper.get_by_id(example_table, example_id, nb_retries=3)

    assert "Server Error" in str(exc_info.value)

    assert mocker.call_count == 4
    assert mocker.last_request.method == "GET"
    assert mocker.last_request.url == expected_url
    assert mocker.last_request.body is None


def test__bubble_wait_correct_amount_of_time(bubble_wrapper, mocker):
    sleep_time = 0.5
    nb_retries = 5

    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(
        expected_url,
        text="Server Error",
        status_code=500,
    )

    start = perf_counter()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_wrapper.get_by_id(
            example_table, example_id, nb_retries=nb_retries, sleep_time=sleep_time
        )
    total_time = perf_counter() - start

    assert "Server Error" in str(exc_info.value)

    assert mocker.call_count == nb_retries + 1
    assert total_time > sleep_time * nb_retries - DELTA_TIME_ERROR


def test__bubble_wait_correct_amount_of_time_with_exponential_backoff(
    bubble_wrapper, mocker
):
    sleep_time = 0.1
    nb_retries = 5
    expected_wait = sum(sleep_time * 2**k for k in range(nb_retries))

    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(
        expected_url,
        text="Server Error",
        status_code=500,
    )

    start = perf_counter()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_wrapper.get_by_id(
            example_table,
            example_id,
            nb_retries=nb_retries,
            sleep_time=sleep_time,
            exponential_backoff=True,
        )
    total_time = perf_counter() - start

    assert "Server Error" in str(exc_info.value)

    assert mocker.call_count == nb_retries + 1
    assert total_time > expected_wait - DELTA_TIME_ERROR


@patch("bubble_api.BubbleWrapper.get_by_id")
def test__use_get_by_id__when__id_is_given(get_by_id, bubble_wrapper):
    bubble_type = "test_type"
    _id = "test_id"

    _ = bubble_wrapper.get(bubble_type, bubble_id=_id)

    assert get_by_id.called_called_with(bubble_type, _id)


@patch("bubble_api.BubbleWrapper.get_objects")
def test__use_get_objects__when__constraints_are_given(get_objects, bubble_wrapper):
    bubble_type = "test_type"
    constraints = [
        Field("Name") == "Bob",
        Field("Age") > 18,
    ]

    _ = bubble_wrapper.get(bubble_type, constriants=constraints)

    assert get_objects.called_called_with(bubble_type, constraints)


@patch("bubble_api.BubbleWrapper.create_object")
def test__use_create_object__when__one_object_is_given(create_object, bubble_wrapper):
    bubble_type = "test_type"
    fields = {"field_a": "abc"}

    _ = bubble_wrapper.create(bubble_type, fields)

    assert create_object.called_called_with(bubble_type, fields)


@patch("bubble_api.BubbleWrapper.create_bulk")
def test__use_create_bulk__when__several_objects_are_given(create_bulk, bubble_wrapper):
    bubble_type = "test_type"
    fields = [{"field_a": "abc"}, {"field_a": "def"}]

    _ = bubble_wrapper.create(bubble_type, fields)

    assert create_bulk.called_called_with(bubble_type, fields)


@patch("bubble_api.BubbleWrapper.delete_by_id")
def test__use_delete_by_id__when__one_id_is_given(delete_by_id, bubble_wrapper):
    bubble_type = "test_type"
    _id = "test_id"

    bubble_wrapper.delete(bubble_type, _id)

    assert delete_by_id.called_called_with(bubble_type, _id)


@patch("bubble_api.BubbleWrapper.delete_by_ids")
def test__use_delete_by_ids__when__several_ids_are_given(delete_by_ids, bubble_wrapper):
    bubble_type = "test_type"
    _ids = ["test_id_1", "test_id_2", "test_id_3"]

    bubble_wrapper.delete(bubble_type, _ids)

    assert delete_by_ids.called_called_with(bubble_type, _ids)


@patch("bubble_api.BubbleWrapper.delete_objects")
def test__use_delete_objects__when__constraints_are_given(
    delete_objects, bubble_wrapper
):
    bubble_type = "test_type"
    constraints = [Field("Name") == "Bob", Field("Age") > 18]

    bubble_wrapper.delete(bubble_type, constraints=constraints)

    assert delete_objects.called_called_with(bubble_type, constraints)


def test__raise_warning__when__only_type_is_given(bubble_wrapper):
    bubble_type = "test_type"

    with pytest.raises(Warning):
        bubble_wrapper.delete(bubble_type)


def test__should__raise_error__when__no_object_found_with_column_specified(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    example_column = "example_column"
    expected_url = f"{OBJ_API_URL_EXAMPLE}/{example_table}"

    mocker.get(
        expected_url,
        json={
            "response": {
                "results": [],
                "count": 0,
                "remaining": 0,
                "cursor": 0
            }
        },
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_wrapper.get_by_id(example_table, example_id, column_name=example_column)
