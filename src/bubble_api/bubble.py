import time
import requests
import json
import pandas as pd
from requests.exceptions import ConnectionError
from requests.exceptions import ReadTimeout
from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator
from pydantic import validate_arguments
from pydantic.typing import Annotated
from typing import Optional, List, Literal, Any
from threading import Thread


BUBBLE_API_VERSION = "1.1"

# Number of retries
DEF_RETRIES = 0
MIN_RETRIES = 0
MAX_RETRIES = 10

# Wait time between request retries
DEF_WAIT_TIME = 0
MIN_WAIT_TIME = 0

# Verbose level
DEF_VERB_LEV = 1
MIN_VERB_LEV = 0
MAX_VERB_LEV = 2


def save_result_in_dict(save_in_dict=None, save_in_key=None):
    """
    This decorator is used to save the result of the decorated function in a dictionary
    """
    if save_in_dict is not None and save_in_key is None:
        raise ValueError("save_in_key must be provided if save_result_in_dict is provided")

    def decorator(function):
        def new_function(*args, **kwargs):
            result = function(*args, **kwargs)
            if save_in_dict is not None:
                save_in_dict[save_in_key] = result
            return result
        return new_function
    return decorator


class Constraint(BaseModel):
    """
    See https://manual.bubble.io/core-resources/api/data-api#search-constraints for more details
    """
    key: str
    constraint_type: Literal[
        "equals",
        "not equal",
        "is_empty",
        "is_not_empty",
        "text contains",
        "not text contains",
        "greater than",
        "less than",
        "in",
        "not int",
        "contains",
        "not contains",
        "empty",
        "not empty",
        "geographic_search"
    ]
    value: Any = None


class GetDataResp(BaseModel):
    results: List[dict]
    remaining: int = 0
    count: int

    def __str__(self):
        return self.json(indent=4)


class GetFullDataResp(BaseModel):
    results: Optional[List[dict]] = None
    path_to_file: Optional[str] = None

    def __str__(self):
        return self.json(indent=4)


class Bubble(BaseModel):

    base_url: str = Field(
        ...,
        description="URL to your Bubble website (https://{DOMAIN})"
    )
    bubble_version: Optional[str] = Field(
        "live",
        description="Version of your website to use. Set test for Development branch"
    )
    api_key: Optional[str] = Field(
        None,
        description="API key. It can also be a user token"
    )
    n_retries: int = Field(
        DEF_RETRIES,
        ge=MIN_RETRIES,
        le=MAX_RETRIES,
        description="Maximum number of retries in case of request failure"
    )
    base_wait_time: int = Field(
        DEF_WAIT_TIME,
        ge=MIN_WAIT_TIME,
        description="Time to wait between retries in seconds. "
                    "If set to 0, do not wait between retries. "
                    "Must be greater than 0 if exponential_backoff is True"
    )
    exponential_backoff: bool = Field(
        False,
        description="Whether to use exponential backoff for time between retries. "
                    "If exponential_backoff is True, no wait time after first try and then wait time is "
                    "(base_wait_time^n) where n >= 1 is the retry index"
    )
    verbose_level: int = Field(
        DEF_VERB_LEV,
        ge=MIN_VERB_LEV,
        le=MAX_VERB_LEV,
        description="If set to 0. Do not print anything. "
                    "If set to 1, print information in case of failure. Default and recommended value for "
                    "production environments"
                    "If set to 2, print lots of information. Not recommended in production environments"
    )
    timeout: Optional[int] = Field(
        None,
        ge=1,
        description="Use this parameter to set request timeout in seconds. "
                    "If no response is received after the given time a ConnectionError is raised. "
                    "Be extremely careful when you use this parameter and modify or create objects in your database. "
                    "A timeout does not necessarily mean that the object has not been modified or created"
    )

    @root_validator()
    def exponential_backoff_time(cls, values):
        if values.get("exponential_backoff", False) and values.get("base_wait_time", 0) <= 1:
            raise ValueError("base_wait_time must be greater than 1 if exponential_backoff is True")
        return values

    def __str__(self):
        if self.api_key is not None:
            # The whole key is printed if its length is less than 3
            # (not an issue because it means it's not a real key)
            api_key_info = self.api_key[:3] + "*" * max(len(self.api_key) - 3, 0)

        else:
            api_key_info = "No API key provided"

        return f"Bubble instance ({api_key_info})"

    @validate_arguments
    def make_request(
            self,
            bubble_type: str,
            unique_id: Optional[str] = None,
            cursor: Optional[Annotated[int, Field(ge=0)]] = 0,
            limit: Optional[Annotated[int, Field(ge=1, le=100)]] = 100,
            sort_field: Optional[str] = None,
            descending: bool = False,
            constraints: Optional[List[Constraint]] = None,
            columns_selected: Optional[List[str]] = None,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None,
            exclude_remaining: Optional[bool] = False
    ) -> GetDataResp:
        """
        This function is used to make GET requests to Bubble and handle errors and retries.
        (!) It doesn't handle pagination (!)
        See https://manual.bubble.io/core-resources/api/data-api#getting-a-list-of-things-and-search for more details
        :param bubble_type: Name of the table to request
        :param unique_id: Unique id of the object to retrieve. If provided, ignore search constraints, and pagination
        parameters. To directly get object without pagination information, use method get_object_by_id
        :param cursor: Rank of the first item in the list to fetch
        :param limit: Maximum number of items in response
        :param sort_field: Field to use for sorting. No sort if not provided
        :param descending: True for descending sorting, False for ascending sorting
        :param constraints: List of Constraint
        :param columns_selected: List of columns to return in response. If not provided, return all columns
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        :param exclude_remaining: If set to True, remaining is an estimation and not the exact count
        If None, use self.timeout
        """
        if n_retries is None:
            n_retries = self.n_retries
        if base_wait_time is None:
            base_wait_time = self.base_wait_time
        if exponential_backoff is None:
            exponential_backoff = self.exponential_backoff
        if verbose_level is None:
            verbose_level = self.verbose_level
        if timeout is None:
            timeout = self.timeout
        if timeout == 0:
            timeout = None

        if exponential_backoff and base_wait_time <= 1:
            raise ValueError("base_wait_time must be greater than 1 if exponential_backoff is True")

        base_url = f"{self.base_url}/version-{self.bubble_version}" if self.bubble_version != "live" else \
            self.base_url

        if unique_id is not None:
            full_url = f"{base_url}/api/{BUBBLE_API_VERSION}/obj/{bubble_type}/{unique_id}"
            params = {}

        else:
            full_url = f"{base_url}/api/{BUBBLE_API_VERSION}/obj/{bubble_type}"
            params = {
                "limit": limit,
                "cursor": cursor,
                "constraints": json.dumps([c.dict() for c in constraints]) if constraints is not None else None,
                "sort_field": sort_field,
                "descending": descending,
                "exclude_remaining": exclude_remaining
            }

        if verbose_level >= 2:
            print("GET request URL : ", full_url)
            print("GET request parameters : ", json.dumps(params, indent=4))

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key is not None else None

        break_while = False
        retry_index = 0
        resp = None
        no_resp_error = "Unknown error"

        while retry_index <= n_retries and not break_while:
            try:
                if (retry_index >= 1 or verbose_level >= 2) and verbose_level >= 1:
                    print(f"GET request - Retry index : {retry_index}/{n_retries}")

                resp = requests.get(
                    full_url,
                    headers=headers,
                    params=params,
                    timeout=timeout
                )

                if resp.ok:
                    break_while = True
                    if verbose_level >= 2:
                        print("GET request - Success - Response content :", str(resp.content))

                    json_resp = resp.json()
                    response = json_resp.get("response", {})
                    if unique_id is not None:
                        results = [response]
                        remaining = 0
                        resp_count = 1

                    else:
                        results = response.get("results", [])
                        remaining = response.get("remaining", 0)
                        resp_count = response.get("count", 0)

                    if columns_selected is not None and len(columns_selected) > 0:
                        results = [{column: result.get(column) for column in columns_selected} for result in results]

                    return GetDataResp(
                        results=results,
                        remaining=remaining,
                        count=resp_count
                    )

                elif resp.status_code in [400, 404]:
                    break_while = True
                    if resp.status_code == 404 and unique_id is not None:
                        return GetDataResp(
                            results=[],
                            remaining=0,
                            count=0
                        )

                    if verbose_level >= 1:
                        print(f"GET request - {resp.status_code} - {str(resp.content)}")

                    json_resp = resp.json()
                    if "body" in json_resp:
                        raise ValueError(json.dumps(json_resp.get("body"), indent=4))

                elif resp.status_code == 401:  # Unauthorized
                    break_while = True
                    if verbose_level >= 1:
                        print(f"GET request - 401 - {str(resp.content)}")

                    json_resp = resp.json()
                    if "translation" in json_resp:
                        raise ValueError(json_resp.get("translation"))

                else:
                    if verbose_level >= 1:
                        print(f"GET request - {resp.status_code} - {str(resp.content)}")

            except AttributeError as ae:  # If resp.json() fails
                if verbose_level >= 1:
                    print("GET request - AttributeError", str(ae))

            except ConnectionError as ce:
                no_resp_error = "ConnectionError"
                if verbose_level >= 1:
                    print("GET request - ConnectionError :", str(ce))

            except ReadTimeout as rto:
                no_resp_error = "ReadTimeout"
                if verbose_level >= 1:
                    print("GET request - ReadTimeout :", str(rto))

            finally:
                if not break_while and retry_index < n_retries:
                    if exponential_backoff:
                        sleep_time = 0 if retry_index == 0 else base_wait_time ** retry_index
                    else:
                        sleep_time = base_wait_time

                    if verbose_level >= 1 and sleep_time > 0:
                        print(f"GET request - Wait {sleep_time} second(s)")

                    time.sleep(sleep_time)

                retry_index += 1

        # All requests failed
        if n_retries == 0:
            request_failed = "The request failed"
        else:
            request_failed = "All requests failed"

        if verbose_level >= 1:
            if resp is not None:
                print(f"GET request - {request_failed} - Last response content : {str(resp.content)}")
            else:
                print(f"GET request - {request_failed} - Response is None ({no_resp_error})")

        if resp is not None:
            raise ValueError(f"{request_failed} - Last response content : {str(resp.content)}")
        else:  # Can happen if all requests lead to a ConnectionError or ReadTimeout
            raise ValueError(f"{request_failed} - Response is None ({no_resp_error})")

    def count_items(
            self,
            bubble_type: str,
            constraints: Optional[List[Constraint]] = None,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None
    ) -> int:
        """
        This function is used to count the number of items returned by a query but does not actually get these items
        :param bubble_type: Name of the table to request
        :param constraints: List of Constraint
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        If None, use self.timeout
        """
        get_data_resp = self.make_request(
            bubble_type=bubble_type,
            cursor=0,
            limit=1,
            constraints=constraints,
            n_retries=n_retries,
            base_wait_time=base_wait_time,
            exponential_backoff=exponential_backoff,
            verbose_level=verbose_level,
            timeout=timeout
        )

        return len(get_data_resp.results) + get_data_resp.remaining

    def make_full_request(
            self,
            bubble_type: str,
            limit: Optional[Annotated[int, Field(ge=1, le=100)]] = 100,
            sort_field: Optional[str] = None,
            descending: bool = False,
            constraints: Optional[List[Constraint]] = None,
            columns_selected: Optional[List[str]] = None,
            path_to_file: Optional[str] = None,
            file_format: Literal["CSV"] = "CSV",
            n_threads: Annotated[int, Field(ge=0)] = 0,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None
    ) -> GetFullDataResp:
        """
        This function is used to make GET requests to Bubble and handle errors, retries and pagination.
        The data will be stored in the file provided in path_to_file if any. If no file is provided, the data will
        be returned in a list. In this case, you need to make sure not to query too much data
        :param bubble_type: Name of the table to request
        :param limit: Maximum number of items in response
        :param sort_field: Field to use for sorting. No sort if not provided
        :param descending: True for descending sorting, False for ascending sorting
        :param constraints: List of Constraint
        :param columns_selected: List of columns to return in response. If not provided, return all columns
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param path_to_file: Path to a file where to save data
        :param file_format: Format to use to save data. Only CSV is supported
        :param n_threads: Number of simultaneous requests to make at most. If set to 0, do not use threads
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        If None, use self.timeout
        """
        cursor = 0
        full_results = []
        if n_threads == 0:
            remaining = 1
            first_request = True
            while remaining > 0:
                get_data_resp = self.make_request(
                    bubble_type=bubble_type,
                    cursor=cursor,
                    limit=limit,
                    sort_field=sort_field,
                    descending=descending,
                    constraints=constraints,
                    columns_selected=columns_selected,
                    n_retries=n_retries,
                    base_wait_time=base_wait_time,
                    exponential_backoff=exponential_backoff,
                    verbose_level=verbose_level,
                    timeout=timeout
                )

                if path_to_file is None:
                    full_results.extend(get_data_resp.results)

                else:
                    if file_format == "CSV":
                        df = pd.DataFrame(get_data_resp.results)
                        df.to_csv(
                            path_to_file,
                            mode="w" if first_request else "a",
                            header=first_request,
                            index=False
                        )
                    else:
                        raise ValueError(f"{file_format} is not a valid value for file_format")

                remaining = get_data_resp.remaining
                cursor += get_data_resp.count
                first_request = False

        else:
            n_items = self.count_items(
                bubble_type=bubble_type,
                constraints=constraints,
                n_retries=n_retries,
                base_wait_time=base_wait_time,
                exponential_backoff=exponential_backoff,
                verbose_level=verbose_level,
                timeout=timeout
            )
            n_total_threads = (n_items - 1) // limit + 1
            n_batches = (n_total_threads - 1) // n_threads + 1
            for b in range(n_batches):
                n_remaining_threads = n_total_threads - (b * n_threads)
                threads = []
                result_dict = dict()
                for t in range(min(n_threads, n_remaining_threads)):
                    thread = Thread(
                        target=save_result_in_dict(result_dict, str(b * n_threads + t))(self.make_request),
                        kwargs={
                            "bubble_type": bubble_type,
                            "cursor": cursor,
                            "limit": limit,
                            "sort_field": sort_field,
                            "descending": descending,
                            "constraints": constraints,
                            "columns_selected": columns_selected,
                            "n_retries": n_retries,
                            "base_wait_time": base_wait_time,
                            "exponential_backoff": exponential_backoff,
                            "verbose_level": verbose_level,
                            "timeout": timeout
                        }
                    )
                    thread.start()
                    threads.append(thread)

                    cursor += limit

                # Wait for threads to end
                for thread in threads:
                    thread.join()

                for t in range(min(n_threads, n_remaining_threads)):
                    get_data_resp = result_dict.get(str(b * n_threads + t))
                    if get_data_resp is None:
                        raise ValueError("Could not fetch data")

                    if path_to_file is None:
                        full_results.extend(get_data_resp.results)

                    else:
                        if file_format == "CSV":
                            df = pd.DataFrame(get_data_resp.results)
                            df.to_csv(
                                path_to_file,
                                mode="w" if b * n_threads + t == 0 else "a",
                                header=b * n_threads + t == 0,
                                index=False
                            )
                        else:
                            raise ValueError(f"{file_format} is not a valid value for file_format")

        return GetFullDataResp(
            results=None if path_to_file is not None else full_results,
            path_to_file=path_to_file
        )

    def get_object_by_id(
            self,
            bubble_type: str,
            unique_id: str,
            column_id: str = "_id",
            fail_if_multiple_results: bool = False,
            fail_if_not_found: bool = False,
            columns_selected: Optional[List[str]] = None,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None
    ) -> Optional[dict]:
        """
        This function is used to retrieve an object using an identifier. By default, it uses _id. You can change
        this behavior providing column_id. You should only use columns with unique constraint.
        If multiple objects can be found, only one is randomly returned. Note that using _id is quicker
        :param bubble_type: Name of the table to request
        :param unique_id: If of the object to fetch
        :param column_id: The column to search
        :param fail_if_multiple_results: If set to True, fail if more than one object is found
        :param fail_if_not_found: If set to True, fail if no object if found
        :param columns_selected: List of columns to return in response. If not provided, return all columns
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        If None, use self.timeout
        """
        if column_id == "_id" or "unique_id":
            get_data_resp = self.make_request(
                bubble_type=bubble_type,
                unique_id=unique_id,
                columns_selected=columns_selected,
                n_retries=n_retries,
                base_wait_time=base_wait_time,
                exponential_backoff=exponential_backoff,
                verbose_level=verbose_level,
                timeout=timeout
            )

        else:
            constraints = [
                Constraint(
                    key=column_id,
                    constraint_type="equals",
                    value=unique_id
                )
            ]

            get_data_resp = self.make_request(
                bubble_type=bubble_type,
                cursor=0,
                limit=1,
                constraints=constraints,
                columns_selected=columns_selected,
                n_retries=n_retries,
                base_wait_time=base_wait_time,
                exponential_backoff=exponential_backoff,
                verbose_level=verbose_level,
                timeout=timeout,
                exclude_remaining=False if fail_if_multiple_results else True
            )

        if fail_if_multiple_results and get_data_resp.remaining > 0:
            raise ValueError("More than one object found")

        results = get_data_resp.results

        if fail_if_not_found and len(results) == 0:
            raise ValueError(f"Object not found in table {bubble_type} ({column_id} = {unique_id})")

        return results[0] if len(results) > 0 else None

    def update_object(
            self,
            bubble_type: str,
            unique_id: str,
            params: dict,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None
    ) -> str:
        """
        This function is used to make PATCH requests to Bubble and handle errors and retries.
        https://manual.bubble.io/core-resources/api/data-api#modify-a-thing-by-id for more details
        :param bubble_type: Name of the table to request
        :param unique_id: Unique id of the object to update
        :param params: Dictionary with updated attributes. Columns that are not in this dictionary won't be updated
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        If None, use self.timeout
        """
        if n_retries is None:
            n_retries = self.n_retries
        if base_wait_time is None:
            base_wait_time = self.base_wait_time
        if exponential_backoff is None:
            exponential_backoff = self.exponential_backoff
        if verbose_level is None:
            verbose_level = self.verbose_level
        if timeout is None:
            timeout = self.timeout
        if timeout == 0:
            timeout = None

        if exponential_backoff and base_wait_time <= 1:
            raise ValueError("base_wait_time must be greater than 1 if exponential_backoff is True")

        base_url = f"{self.base_url}/version-{self.bubble_version}" if self.bubble_version != "live" else \
            self.base_url

        full_url = f"{base_url}/api/{BUBBLE_API_VERSION}/obj/{bubble_type}/{unique_id}"

        if verbose_level >= 2:
            print("PATCH request URL : ", full_url)
            print("PATCH request parameters : ", json.dumps(params, indent=4))

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key is not None else None

        break_while = False
        retry_index = 0
        resp = None
        no_resp_error = "Unknown error"

        while retry_index <= n_retries and not break_while:
            try:
                if (retry_index >= 1 or verbose_level >= 2) and verbose_level >= 1:
                    print(f"PATCH request - Retry index : {retry_index}/{n_retries}")

                resp = requests.patch(
                    full_url,
                    headers=headers,
                    json=params,
                    timeout=timeout
                )

                if resp.ok:
                    if verbose_level >= 2:
                        print(f"PATCH request - Object {unique_id} of table {bubble_type} updated")

                    return unique_id

                elif resp.status_code in [400, 404]:
                    break_while = True
                    if verbose_level >= 1:
                        print(f"PATCH request - {resp.status_code} - {str(resp.content)}")

                    json_resp = resp.json()
                    if "body" in json_resp:
                        raise ValueError(json.dumps(json_resp.get("body"), indent=4))

                elif resp.status_code == 401:  # Unauthorized
                    break_while = True
                    if verbose_level >= 1:
                        print(f"PATCH request - 401 - {str(resp.content)}")

                    json_resp = resp.json()
                    if "translation" in json_resp:
                        raise ValueError(json_resp.get("translation"))

                else:
                    if verbose_level >= 1:
                        print(f"PATCH request - {resp.status_code} - {str(resp.content)}")

            except AttributeError as ae:  # If resp.json() fails
                if verbose_level >= 1:
                    print("PATCH request - AttributeError", str(ae))

            except ConnectionError as ce:
                no_resp_error = "ConnectionError"
                if verbose_level >= 1:
                    print("PATCH request - ConnectionError :", str(ce))

            except ReadTimeout as rto:
                no_resp_error = "ReadTimeout"
                if verbose_level >= 1:
                    print("PATCH request - ReadTimeout :", str(rto))

            finally:
                if not break_while and retry_index < n_retries:
                    if exponential_backoff:
                        sleep_time = 0 if retry_index == 0 else base_wait_time ** retry_index
                    else:
                        sleep_time = base_wait_time

                    if verbose_level >= 1 and sleep_time > 0:
                        print(f"PATCH request - Wait {sleep_time} second(s)")

                    time.sleep(sleep_time)

                retry_index += 1

        # All requests failed
        if n_retries == 0:
            request_failed = "The request failed"
        else:
            request_failed = "All requests failed"

        if verbose_level >= 1:
            if resp is not None:
                print(f"PATCH request - {request_failed} - Last response content : {str(resp.content)}")
            else:
                print(f"PATCH request - {request_failed} - Response is None ({no_resp_error})")

        if resp is not None:
            raise ValueError(f"{request_failed} - Last response content : {str(resp.content)}")
        else:  # Can happen if all requests lead to a ConnectionError or ReadTimeout
            raise ValueError(f"{request_failed} - Response is None ({no_resp_error})")

    def create_object(
            self,
            bubble_type: str,
            params: dict,
            n_retries: Optional[Annotated[int, Field(ge=MIN_RETRIES, le=MAX_RETRIES)]] = None,
            base_wait_time: Optional[Annotated[int, Field(ge=MIN_WAIT_TIME)]] = None,
            exponential_backoff: Optional[bool] = None,
            verbose_level: Optional[Annotated[int, Field(ge=MIN_VERB_LEV, le=MAX_VERB_LEV)]] = None,
            timeout: Optional[Annotated[int, Field(ge=0)]] = None
    ) -> str:
        """
        This function is used to make POST requests to Bubble and handle errors and retries.
        https://manual.bubble.io/core-resources/api/data-api#create-a-new-thing for more details
        :param bubble_type: Name of the table to request
        :param params: Dictionary with updated attributes. Columns that are not in this dictionary won't be updated
        :param n_retries: Overrides parent value. Maximum number of retries in case of request failure.
        If None, use self.n_retries
        :param base_wait_time: Overrides parent value. Time to wait between retries in seconds.
        If None, use self.base_wait_time
        :param exponential_backoff: Overrides parent value. Whether to use exponential backoff for time between retries.
        If None, use self.exponential_backoff
        :param verbose_level: Overrides parent value. Verbose level.
        If None, use self.verbose_level
        :param timeout: Overrides parent value. Use this parameter to set request timeout in seconds. Set 0 to remove
        timeout. (!) None will not erase the parent value (!).
        If None, use self.timeout
        """
        if n_retries is None:
            n_retries = self.n_retries
        if base_wait_time is None:
            base_wait_time = self.base_wait_time
        if exponential_backoff is None:
            exponential_backoff = self.exponential_backoff
        if verbose_level is None:
            verbose_level = self.verbose_level
        if timeout is None:
            timeout = self.timeout
        if timeout == 0:
            timeout = None

        if exponential_backoff and base_wait_time <= 1:
            raise ValueError("base_wait_time must be greater than 1 if exponential_backoff is True")

        base_url = f"{self.base_url}/version-{self.bubble_version}" if self.bubble_version != "live" else \
            self.base_url

        full_url = f"{base_url}/api/{BUBBLE_API_VERSION}/obj/{bubble_type}"

        if verbose_level >= 2:
            print("POST request URL : ", full_url)

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key is not None else None

        if verbose_level >= 2:
            print("POST request parameters : ", json.dumps(params, indent=4))

        break_while = False
        retry_index = 0
        resp = None
        no_resp_error = "Unknown error"

        while retry_index <= n_retries and not break_while:
            try:
                if (retry_index >= 1 or verbose_level >= 2) and verbose_level >= 1:
                    print(f"POST request - Retry index : {retry_index}/{n_retries}")

                resp = requests.post(
                    full_url,
                    headers=headers,
                    json=params,
                    timeout=timeout
                )

                if resp.ok:
                    if verbose_level >= 2:
                        print(f"POST request - {resp.status_code} - {str(resp.content)}")

                    json_resp = resp.json()
                    return json_resp.get("id")

                elif resp.status_code in [400, 404]:
                    break_while = True
                    if verbose_level >= 1:
                        print(f"POST request - {resp.status_code} - {str(resp.content)}")

                    json_resp = resp.json()
                    if "body" in json_resp:
                        raise ValueError(json.dumps(json_resp.get("body"), indent=4))

                elif resp.status_code == 401:  # Unauthorized
                    break_while = True
                    if verbose_level >= 1:
                        print(f"POST request - 401 - {str(resp.content)}")

                    json_resp = resp.json()
                    if "translation" in json_resp:
                        raise ValueError(json_resp.get("translation"))

                else:
                    if verbose_level >= 1:
                        print(f"POST request - {resp.status_code} - {str(resp.content)}")

            except AttributeError as ae:  # If resp.json() fails
                if verbose_level >= 1:
                    print("POST request - AttributeError", str(ae))

            except ConnectionError as ce:
                no_resp_error = "ConnectionError"
                if verbose_level >= 1:
                    print("POST request - ConnectionError :", str(ce))

            except ReadTimeout as rto:
                no_resp_error = "ReadTimeout"
                if verbose_level >= 1:
                    print("POST request - ReadTimeout :", str(rto))

            finally:
                if not break_while and retry_index < n_retries:
                    if exponential_backoff:
                        sleep_time = 0 if retry_index == 0 else base_wait_time ** retry_index
                    else:
                        sleep_time = base_wait_time

                    if verbose_level >= 1 and sleep_time > 0:
                        print(f"POST request - Wait {sleep_time} second(s)")

                    time.sleep(sleep_time)

                retry_index += 1

        # All requests failed
        if n_retries == 0:
            request_failed = "The request failed"
        else:
            request_failed = "All requests failed"

        if verbose_level >= 1:
            if resp is not None:
                print(f"POST request - {request_failed} - Last response content : {str(resp.content)}")
            else:
                print(f"POST request - {request_failed} - Response is None ({no_resp_error})")

        if resp is not None:
            raise ValueError(f"{request_failed} - Last response content : {str(resp.content)}")
        else:  # Can happen if all requests lead to a ConnectionError or ReadTimeout
            raise ValueError(f"{request_failed} - Response is None ({no_resp_error})")
