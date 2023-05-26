import os

import pytest
import requests

from bubble_api import BubbleWrapper, Field


@pytest.fixture(scope="session")
def bubble_wrapper():
    return BubbleWrapper(
        base_url="https://cuure.com",
        api_key=os.environ["BUBBLE_API_KEY"],
        bubble_version="test",
    )


def clean_test_data(bubble_wrapper):
    bubble_wrapper.delete_objects(
        "appfeedback", Field("app_version").text_contains("test")
    )


@pytest.fixture(scope="session", autouse=True)
def cleaning_feedback_data(bubble_wrapper):
    clean_test_data(bubble_wrapper)
    yield
    clean_test_data(bubble_wrapper)


def test__create_object(bubble_wrapper):
    object_id = bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.0 test"},
    )

    assert isinstance(object_id, str)


def test__get_object_by_id(bubble_wrapper):
    object_id = bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.1 test"},
    )

    feedback = bubble_wrapper.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["app_version"] == "2.2.1 test"


def test__delete_object_by_id(bubble_wrapper):
    object_id = bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.2 test"},
    )

    bubble_wrapper.get_by_id(
        "appfeedback",
        object_id,
    )

    bubble_wrapper.delete_by_id(
        "appfeedback",
        object_id,
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_wrapper.get_by_id(
            "appfeedback",
            object_id,
        )

    assert "404 Client Error: Not Found " in str(exc_info.value)


def test__update_object(bubble_wrapper):
    object_id = bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.3 test"},
    )

    bubble_wrapper.update_object(
        "appfeedback",
        object_id,
        {"rating": 4, "app_version": "2.2.3 test"},
    )

    feedback = bubble_wrapper.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["rating"] == 4
    assert feedback["app_version"] == "2.2.3 test"


def test__replace_object(bubble_wrapper):
    object_id = bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.4 test"},
    )

    bubble_wrapper.replace_object(
        "appfeedback",
        object_id,
        {"app_version": "2.3.4 test"},
    )

    feedback = bubble_wrapper.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert "rating" not in feedback
    assert feedback["app_version"] == "2.3.4 test"


def test__count_objects_with_constraints(bubble_wrapper):
    bubble_wrapper.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.5 test"},
    )

    bubble_wrapper.create(
        "appfeedback",
        {"rating": 4, "app_version": "2.2.5 test"},
    )

    bubble_wrapper.create(
        "appfeedback",
        {"rating": 3, "app_version": "2.2.5 test"},
    )

    res = bubble_wrapper.count_objects(
        "appfeedback",
        Field("app_version") == "2.2.5 test",
    )

    assert res == 3


def test__get_objects_with_constraints(bubble_wrapper):
    id_12 = bubble_wrapper.create(
        "appfeedback",
        {"rating": 12, "app_version": "2.2.6 test"},
    )

    id_11 = bubble_wrapper.create(
        "appfeedback",
        {"rating": 11, "app_version": "2.2.6 test"},
    )

    bubble_wrapper.create(
        "appfeedback",
        {"rating": 10, "app_version": "2.2.6 test"},
    )

    res = bubble_wrapper.get_objects(
        "appfeedback",
        [Field("rating") > 10, Field("app_version").text_contains(" test")],
    )

    assert set([r["_id"] for r in res]) == {id_11, id_12}


def test__create_bulk(bubble_wrapper):
    bulk_res = bubble_wrapper.create_bulk(
        "appfeedback",
        [
            {"rating": 9, "app_version": "2.2.7 test"},
            {"rating": 8, "app_version": "2.2.7 test"},
            {"rating": 7, "app_version": "2.2.7 test"},
        ],
    )

    assert all(r["status"] == "success" for r in bulk_res)

    res = bubble_wrapper.get_objects(
        "appfeedback",
        Field("app_version") == "2.2.7 test",
    )

    assert set([r["_id"] for r in res]) == set(r["id"] for r in bulk_res)
