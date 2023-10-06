import pytest
import httpx

from bubble_api import Field


@pytest.mark.integration
async def test__create_object(async_bubble_client):
    object_id = await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.0 test"},
    )

    assert isinstance(object_id, str)


@pytest.mark.integration
async def test__get_object_by_id(async_bubble_client):
    object_id = await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.1 test"},
    )

    feedback = await async_bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["app_version"] == "async 2.2.1 test"


@pytest.mark.integration
async def test__delete_object_by_id(async_bubble_client):
    object_id = await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.2 test"},
    )

    obj = await async_bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert obj["_id"] == object_id

    await async_bubble_client.delete_by_id(
        "appfeedback",
        object_id,
    )

    with pytest.raises(httpx.HTTPError) as exc_info:
        await async_bubble_client.get_by_id(
            "appfeedback",
            object_id,
        )

    assert "Client error '404 Not Found'" in str(exc_info.value)


@pytest.mark.integration
async def test__update_object(async_bubble_client):
    object_id = await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.3 test"},
    )

    await async_bubble_client.update_object(
        "appfeedback",
        object_id,
        {"rating": 4, "app_version": "async 2.2.3 test"},
    )

    feedback = await async_bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert feedback["rating"] == 4
    assert feedback["app_version"] == "async 2.2.3 test"


@pytest.mark.integration
async def test__replace_object(async_bubble_client):
    object_id = await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.4 test"},
    )

    await async_bubble_client.replace_object(
        "appfeedback",
        object_id,
        {"app_version": "2.3.4 test"},
    )

    feedback = await async_bubble_client.get_by_id(
        "appfeedback",
        object_id,
    )

    assert isinstance(feedback, dict)
    assert "rating" not in feedback
    assert feedback["app_version"] == "2.3.4 test"
    assert feedback["_id"] == object_id


@pytest.mark.integration
async def test__count_objects_with_constraints(async_bubble_client):
    await async_bubble_client.create(
        "appfeedback",
        {"rating": 5, "app_version": "async 2.2.5 test"},
    )

    await async_bubble_client.create(
        "appfeedback",
        {"rating": 4, "app_version": "async 2.2.5 test"},
    )

    await async_bubble_client.create(
        "appfeedback",
        {"rating": 3, "app_version": "async 2.2.5 test"},
    )

    res = await async_bubble_client.count_objects(
        "appfeedback",
        Field("app_version") == "async 2.2.5 test",
    )

    assert res == 3


@pytest.mark.integration
async def test__get_objects_with_constraints(async_bubble_client):
    id_12 = await async_bubble_client.create(
        "appfeedback",
        {"rating": 12, "app_version": "async 2.2.6 test"},
    )

    id_11 = await async_bubble_client.create(
        "appfeedback",
        {"rating": 11, "app_version": "async 2.2.6 test"},
    )

    await async_bubble_client.create(
        "appfeedback",
        {"rating": 10, "app_version": "async 2.2.6 test"},
    )

    res = await async_bubble_client.get_objects(
        "appfeedback",
        [Field("rating") > 10, Field("app_version").text_contains(" test")],
    )

    assert {r["_id"] for r in res} == {id_11, id_12}


@pytest.mark.integration
async def test__create_bulk(async_bubble_client):
    bulk_res = await async_bubble_client.create_bulk(
        "appfeedback",
        [
            {"rating": 9, "app_version": "async 2.2.7 test"},
            {"rating": 8, "app_version": "async 2.2.7 test"},
            {"rating": 7, "app_version": "async 2.2.7 test"},
        ],
    )

    assert all(r["status"] == "success" for r in bulk_res)

    res = await async_bubble_client.get_objects(
        "appfeedback",
        Field("app_version") == "async 2.2.7 test",
    )

    assert len(res) == 3
    assert {r["_id"] for r in res} == {r["id"] for r in bulk_res}
    assert {r["rating"] for r in res} == {7, 8, 9}
