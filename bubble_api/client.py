from __future__ import annotations

import json
import time
from collections.abc import Iterable
from itertools import islice

import requests

from .constraint import Constraint
from .field import Field

API_VERSION = "1.1"


class BubbleClient:
    def __init__(
        self,
        base_url: str,
        api_token: str | None = None,
        bubble_version: str = "test",
        *args,
        **kwargs,
    ):
        if bubble_version != "live":
            base_url = f"{base_url}/version-{bubble_version}"
        self.base_url = f"{base_url}/api/{API_VERSION}"
        self.api_token = api_token

    def _get_headers(self) -> dict | None:
        return (
            {
                "Authorization": f"Bearer {self.api_token}",
            }
            if self.api_token is not None
            else None
        )

    @staticmethod
    def _format_constraints(
        constraints: Constraint | Iterable[Constraint] | None = None,
    ) -> str:
        if constraints is None:
            constraints = list()
        if isinstance(constraints, Constraint):
            constraints = [constraints]
        return json.dumps([constraint.to_dict() for constraint in constraints])

    @staticmethod
    def format_bubble_type(bubble_type: str):
        return bubble_type.lower().replace(" ", "")

    def make_request(
        self,
        nb_retries: int = 3,
        sleep_time: float = 0.2,
        exponential_backoff: bool = False,
        **kwargs,
    ) -> requests.Response:
        if kwargs.get("headers") is None:
            kwargs["headers"] = self._get_headers()

        response = requests.request(**kwargs)

        if response.status_code // 100 == 2:
            return response

        if nb_retries == 0 or response.status_code // 100 == 4:
            print(response.text)
            response.raise_for_status()

        time.sleep(sleep_time)

        if exponential_backoff:
            sleep_time *= 2

        return self.make_request(
            nb_retries=nb_retries - 1,
            sleep_time=sleep_time,
            exponential_backoff=exponential_backoff,
            **kwargs,
        )

    def get(
        self,
        bubble_type: str,
        bubble_id: str | None = None,
        column_name: str | None = None,
        constraints: list[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        if bubble_id is not None:
            return self.get_by_id(bubble_type, bubble_id, column_name, **kwargs)
        return self.get_objects(bubble_type, constraints, **kwargs)

    def create(self, bubble_type: str, fields: dict | list[dict] = None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        if isinstance(fields, list):
            return self.create_bulk(bubble_type, fields=fields, **kwargs)
        return self.create_object(bubble_type, fields, **kwargs)

    def delete(
        self,
        bubble_type: str,
        bubble_id: str | Iterable[str] | None = None,
        constraints: Constraint | Iterable[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        if bubble_id is not None and constraints is not None:
            raise TypeError("You can't specify both bubble_id and constraints.")
        if isinstance(bubble_id, str):
            return self.delete_by_id(bubble_type, bubble_id, **kwargs)
        if isinstance(bubble_id, Iterable):
            return self.delete_by_ids(bubble_type, bubble_id, **kwargs)
        if constraints is not None:
            return self.delete_objects(bubble_type, constraints, **kwargs)
        raise Warning(
            "You must specify at least one of bubble_id, bubble_ids or constraints.",
            "If you intend to delete the whole table, please use the delete_all method.",
        )

    def get_by_id(self, bubble_type, bubble_id, column_name=None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        if column_name is not None:
            objs = self.get_objects(
                bubble_type,
                [Field(column_name) == bubble_id],
            )
            return objs[0] if len(objs) > 0 else None

        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        resp = self.make_request(method="GET", url=url, **kwargs)

        return resp.json()["response"]

    def create_object(self, bubble_type: str, fields: dict | None = None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}"

        resp = self.make_request(method="POST", url=url, json=fields, **kwargs)

        return resp.json()["id"]

    def update_object(self, bubble_type: str, bubble_id: str, fields: dict, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        self.make_request(method="PATCH", url=url, json=fields, **kwargs)

    def replace_object(self, bubble_type: str, bubble_id: str, fields: dict, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        self.make_request(method="PUT", url=url, json=fields, **kwargs)

    def delete_by_id(self, bubble_type: str, bubble_id: str, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        self.make_request(method="DELETE", url=url, **kwargs)

    def delete_by_ids(self, bubble_type: str, ids: Iterable[str], **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        for _id in ids:
            self.delete_by_id(bubble_type, _id, **kwargs)

    def delete_objects(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        self.delete_by_ids(
            bubble_type,
            (
                obj["_id"]
                for obj in self.get_objects_gen(bubble_type, constraints, **kwargs)
            ),
            **kwargs,
        )

    def delete_all(self, bubble_type, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        self.delete_objects(bubble_type, list(), **kwargs)

    def create_bulk(self, bubble_type: str, fields: list[dict], **kwargs) -> list:
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/bulk"
        headers = {
            **self._get_headers(),
            "Content-Type": "text/plain",
        }

        resp = self.make_request(
            method="POST",
            url=url,
            data="\n".join(json.dumps(f) for f in fields),
            headers=headers,
            **kwargs,
        )

        return [json.loads(r) for r in resp.text.split("\n")]

    def count_objects(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}"
        constraints = self._format_constraints(constraints)

        params = {
            "constraints": constraints,
            "cursor": 0,
            "limit": 1,
        }

        resp = self.make_request(method="GET", url=url, params=params, **kwargs)
        json_resp = resp.json()["response"]

        return json_resp["count"] + json_resp["remaining"]

    def get_objects_gen(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        sort_field: str = None,
        descending: bool = False,
        limit: int = 100,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}"

        constraints = self._format_constraints(constraints)

        if isinstance(sort_field, Field):
            sort_field = sort_field.field_name

        params = {
            "constraints": constraints,
            "cursor": 0,
            "limit": limit,
            "sort_field": sort_field,
            "descending": descending,
        }

        while True:
            resp = self.make_request(method="GET", url=url, params=params, **kwargs)
            json_resp = resp.json()["response"]
            yield from json_resp["results"]

            params["cursor"] = json_resp["cursor"] + json_resp["count"]

            if json_resp["remaining"] == 0:
                break

    def get_objects(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        max_objects: int | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        return list(
            islice(
                self.get_objects_gen(bubble_type, constraints, **kwargs), max_objects
            )
        )

    def run_workflow(
        self, wf_name: str, params: dict | None = None, method: str = "POST", **kwargs
    ):
        url = f"{self.base_url}/wf/{wf_name}"

        resp = self.make_request(method=method, url=url, json=params, **kwargs)

        return resp.json()["response"]
