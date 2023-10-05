import pytest
import os

from bubble_api import BubbleClient, Field
from bubble_api.asyncio import BubbleClient as AsyncBubbleClient


@pytest.fixture(scope="session")
def bubble_client():
    return BubbleClient(
        base_url="https://cuure.com",
        api_token=os.environ["BUBBLE_API_KEY"],
        bubble_version="test",
    )


@pytest.fixture(scope="session")
def async_bubble_client():
    return AsyncBubbleClient(
        base_url="https://cuure.com",
        api_token=os.environ["BUBBLE_API_KEY"],
        bubble_version="test",
    )


@pytest.fixture(scope="session", autouse=True)
def cleaning_feedback_data(bubble_client):
    clean_test_data(bubble_client)
    yield
    clean_test_data(bubble_client)


def clean_test_data(bubble_client):
    bubble_client.delete(
        "appfeedback", constraints=[
            Field("app_version").text_contains("test"),
        ]
    )
