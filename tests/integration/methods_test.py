import os

import pytest
import requests

from bubble_api import Field


@pytest.mark.integration
def test__create_object(bubble_client):
    object_id = bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.0 test"},
    )

    assert isinstance(object_id, str)


@pytest.mark.integration
def test__get_object_by_id(bubble_client):
    object_id = bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.1 test"},
    )

    feedback = bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["app_version"] == "2.2.1 test"


@pytest.mark.integration
def test__delete_object_by_id(bubble_client):
    object_id = bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.2 test"},
    )

    obj = bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert obj["_id"] == object_id

    bubble_client.delete_by_id(
        "appfeedback",
        object_id,
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        bubble_client.get_by_id(
            "appfeedback",
            object_id,
        )

    assert "404 Client Error: Not Found" in str(exc_info.value)


@pytest.mark.integration
def test__update_object(bubble_client):
    object_id = bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.3 test"},
    )

    bubble_client.update_object(
        "appfeedback",
        object_id,
        {"rating": 4, "app_version": "2.2.3 test"},
    )

    feedback = bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["rating"] == 4
    assert feedback["app_version"] == "2.2.3 test"


@pytest.mark.integration
def test__replace_object(bubble_client):
    object_id = bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.4 test"},
    )

    bubble_client.replace_object(
        "appfeedback",
        object_id,
        {"app_version": "2.3.4 test"},
    )

    feedback = bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert "rating" not in feedback
    assert feedback["app_version"] == "2.3.4 test"
    assert feedback["_id"] == object_id


@pytest.mark.integration
def test__count_objects_with_constraints(bubble_client):
    bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "2.2.5 test"},
    )

    bubble_client.create(
        "appfeedback",
        {"rating": 4, "app_version": "2.2.5 test"},
    )

    bubble_client.create(
        "appfeedback",
        {"rating": 3, "app_version": "2.2.5 test"},
    )

    res = bubble_client.count_objects(
        "appfeedback",
        Field("app_version") == "2.2.5 test",
    )

    assert res == 3


@pytest.mark.integration
def test__get_objects_with_constraints(bubble_client):
    id_12 = bubble_client.create(
        "appfeedback",
        {"rating": 12, "app_version": "2.2.6 test"},
    )

    id_11 = bubble_client.create(
        "appfeedback",
        {"rating": 11, "app_version": "2.2.6 test"},
    )

    bubble_client.create(
        "appfeedback",
        {"rating": 10, "app_version": "2.2.6 test"},
    )

    res = bubble_client.get_objects(
        "appfeedback",
        [Field("rating") > 10, Field("app_version").text_contains(" test")],
    )

    assert {r["_id"] for r in res} == {id_11, id_12}


@pytest.mark.integration
def test__create_bulk(bubble_client):
    bulk_res = bubble_client.create_bulk(
        "appfeedback",
        [
            {"rating": 9, "app_version": "2.2.7 test"},
            {"rating": 8, "app_version": "2.2.7 test"},
            {"rating": 7, "app_version": "2.2.7 test"},
        ],
    )

    assert all(r["status"] == "success" for r in bulk_res)

    res = bubble_client.get_objects(
        "appfeedback",
        Field("app_version") == "2.2.7 test",
    )

    assert len(res) == 3
    assert {r["_id"] for r in res} == {r["id"] for r in bulk_res}
    assert {r["rating"] for r in res} == {7, 8, 9}
