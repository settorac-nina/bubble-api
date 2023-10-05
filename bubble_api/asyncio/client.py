from __future__ import annotations

import json
import time
from collections.abc import Iterable
from itertools import islice

import httpx

from bubble_api.constraint import Constraint
from bubble_api.field import Field
from bubble_api.client import API_VERSION


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

    async def make_request(
        self,
        nb_retries: int = 3,
        sleep_time: float = 0.2,
        exponential_backoff: bool = False,
        **kwargs,
    ) -> httpx.Response:
        async with httpx.AsyncClient as client:
            if kwargs.get("headers") is None:
                kwargs["headers"] = self._get_headers()

            response = await client.request(**kwargs)

            if response.status_code // 100 == 2:
                return response

            if nb_retries == 0 or response.status_code // 100 == 4:
                print(response.text)
                response.raise_for_status()

            time.sleep(sleep_time)

            if exponential_backoff:
                sleep_time *= 2

            return await self.make_request(
                nb_retries=nb_retries - 1,
                sleep_time=sleep_time,
                exponential_backoff=exponential_backoff,
                **kwargs,
            )

    async def get(
        self,
        bubble_type: str,
        bubble_id: str | None = None,
        column_name: str | None = None,
        constraints: list[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        if bubble_id is not None:
            return await self.get_by_id(bubble_type, bubble_id, column_name, **kwargs)
        return await self.get_objects(bubble_type, constraints, **kwargs)

    async def create(self, bubble_type: str, fields: dict | list[dict] = None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        if isinstance(fields, list):
            return await self.create_bulk(bubble_type, fields=fields, **kwargs)
        return await self.create_object(bubble_type, fields, **kwargs)

    async def delete(
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
            return await self.delete_by_id(bubble_type, bubble_id, **kwargs)
        if isinstance(bubble_id, Iterable):
            return await self.delete_by_ids(bubble_type, bubble_id, **kwargs)
        if constraints is not None:
            return await self.delete_objects(bubble_type, constraints, **kwargs)
        raise Warning(
            "You must specify at least one of bubble_id, bubble_ids or constraints.",
            "If you intend to delete the whole table, please use the delete_all method.",
        )

    async def get_by_id(self, bubble_type, bubble_id, column_name=None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        if column_name is not None:
            objs = await self.get_objects(
                bubble_type,
                [Field(column_name) == bubble_id],
            )

            if not objs:
                raise httpx.HTTPError(
                    f"Could not find object with id {bubble_id} in column {column_name}."
                )

            return objs[0]

        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        resp = await self.make_request(method="GET", url=url, **kwargs)

        return resp.json()["response"]

    async def create_object(self, bubble_type: str, fields: dict | None = None, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}"

        resp = await self.make_request(method="POST", url=url, json=fields, **kwargs)

        return resp.json()["id"]

    async def update_object(self, bubble_type: str, bubble_id: str, fields: dict, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        await self.make_request(method="PATCH", url=url, json=fields, **kwargs)

    async def replace_object(self, bubble_type: str, bubble_id: str, fields: dict, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        await self.make_request(method="PUT", url=url, json=fields, **kwargs)

    async def delete_by_id(self, bubble_type: str, bubble_id: str, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/{bubble_id}"

        await self.make_request(method="DELETE", url=url, **kwargs)

    async def delete_by_ids(self, bubble_type: str, ids: Iterable[str], **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        for _id in ids:
            await self.delete_by_id(bubble_type, _id, **kwargs)

    async def delete_objects(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)
        await self.delete_by_ids(
            bubble_type,
            (
                obj["_id"]
                async for obj in self.get_objects_gen(bubble_type, constraints, **kwargs)
            ),
            **kwargs,
        )

    async def delete_all(self, bubble_type, **kwargs):
        bubble_type = self.format_bubble_type(bubble_type)
        await self.delete_objects(bubble_type, list(), **kwargs)

    async def create_bulk(self, bubble_type: str, fields: list[dict], **kwargs) -> list:
        bubble_type = self.format_bubble_type(bubble_type)
        url = f"{self.base_url}/obj/{bubble_type}/bulk"
        headers = {
            **self._get_headers(),
            "Content-Type": "text/plain",
        }

        resp = await self.make_request(
            method="POST",
            url=url,
            data="\n".join(json.dumps(f) for f in fields),
            headers=headers,
            **kwargs,
        )

        return [json.loads(r) for r in resp.text.split("\n")]

    async def count_objects(
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

        resp = await self.make_request(method="GET", url=url, params=params, **kwargs)
        json_resp = resp.json()["response"]

        return json_resp["count"] + json_resp["remaining"]

    async def get_objects_gen(
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
            resp = await self.make_request(method="GET", url=url, params=params, **kwargs)
            json_resp = resp.json()["response"]

            for result in json_resp["results"]:
                yield result

            params["cursor"] = json_resp["cursor"] + json_resp["count"]

            if json_resp["remaining"] == 0:
                break

    async def get_objects(
        self,
        bubble_type: str,
        constraints: Constraint | Iterable[Constraint] | None = None,
        max_objects: int | None = None,
        **kwargs,
    ):
        bubble_type = self.format_bubble_type(bubble_type)

        return list(
            islice(
                (obj async for obj in self.get_objects_gen(bubble_type, constraints, **kwargs)), max_objects
            )
        )

    async def run_workflow(
        self, wf_name: str, params: dict | None = None, method: str = "POST", **kwargs
    ):
        url = f"{self.base_url}/wf/{wf_name}"

        resp = await self.make_request(method=method, url=url, json=params, **kwargs)

        return resp.json()["response"]
