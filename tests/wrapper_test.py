from urllib.parse import urlparse, parse_qs
from time import perf_counter
import json

import requests
import requests_mock
import pytest

from bubble_api import BubbleWrapper, Field


BASE_URL_EXAMPLE = "https://example.com"
API_URL_EXAMPLE = f"{BASE_URL_EXAMPLE}/version-test/api/1.1/obj"


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


def test__bubble_count_objects(bubble_wrapper, mocker):
    example_table = "example_table"
    constraints = [
        Field("Name") == "Bob",
        Field("Age") > 18,
    ]
    expected_url = f"{API_URL_EXAMPLE}/{example_table}"

    mocker.get(
        expected_url,
        json={
            "response": {
                "data": {},
                "count": 100,
                "remaining": 50,
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
    assert json.loads(last_request_params["limit"][0]) == 100
    assert json.loads(last_request_params["cursor"][0]) == 0
    assert mocker.last_request.body is None


def test__bubble_retry_server_error(bubble_wrapper, mocker):
    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"
    mocker.get(
        expected_url,
        [
            {"text": "Server Error", "status_code": 500},
            {"json": {"response": {"data": "test"}, "status_code": 200}},
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
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"
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
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"
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
    assert total_time > sleep_time * nb_retries
    assert total_time < sleep_time * (nb_retries + 1)


def test__bubble_wait_correct_amount_of_time_with_exponential_backoff(
    bubble_wrapper, mocker
):
    sleep_time = 0.1
    nb_retries = 5
    expected_wait = sum(sleep_time * 2**k for k in range(nb_retries))

    example_table = "example_table"
    example_id = "example_id"
    expected_url = f"{API_URL_EXAMPLE}/{example_table}/{example_id}"
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
    assert total_time > expected_wait
    assert total_time < expected_wait + sleep_time
