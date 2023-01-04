# bubble-api
Interactions with Bubble.io in Python made easy

If you have any recommendations or if you want to participate in the project, do not hesitate to contact me at 
settorac.nina@gmail.com

I have not yet written any documentation due to lack of time. 
However, I tried to make the code as simple and readable as possible.

Examples
````
from bubble_api.bubble import Bubble, Constraint


# Instantiate Bubble instance
bubble = Bubble(
    api_key="API_KEY",  # Put your API key here. You can also use a user token
    base_url="https://DOMAIN.com",  # Do not include app version and api/1.1 here
    bubble_version="test",  # Request to development branch
    n_retries=4,
    # Wait 0 second after first fail, 2 after second one, 4 after third on, 8 after forth, ...
    base_wait_time=2,
    exponential_backoff=True
)

# Make a simple GET request
get_data_resp = bubble.make_request(
    bubble_type="user",
    limit=2,
    cursor=15,
    columns_selected=["first_name", "last_name", "_id"]  # If not set, return all columns
)

users = get_data_resp.results
remaining = get_data_resp.remaining
count = get_data_resp.count

# Make a request with constraints and pagination automatically handled and data returned in list
full_data_resp = bubble.make_full_request(
    bubble_type="user",
    sort_field="first_name",
    descending=False,
    columns_selected=["first_name", "last_name", "_id"],  # If not set, return all columns
    constraints=[
        Constraint(
            key="last_name",
            constraint_type="equals",
            value="Dupont"
        )
    ]
)

dupont_users = full_data_resp.results

# Make a request with constraints and pagination automatically handled and data returned saved in a file
full_data_resp_2 = bubble.make_full_request(
    bubble_type="user",
    sort_field="first_name",
    descending=False,
    columns_selected=["first_name", "last_name", "_id"],  # If not set, return all columns
    constraints=[
        Constraint(
            key="last_name",
            constraint_type="equals",
            value="Dupont"
        )
    ],
    path_to_file="dupont_users.csv"
)

path_to_file = full_data_resp_2.path_to_file

# Make a request with constraints and pagination automatically handled and data returned saved in a file and use
# threads to go faster (setting threads to 2 doesn't mean that it will be twice as fast)
full_data_resp_3 = bubble.make_full_request(
    bubble_type="user",
    sort_field="first_name",
    descending=False,
    columns_selected=["first_name", "last_name", "_id"],  # If not set, return all columns
    constraints=[
        Constraint(
            key="last_name",
            constraint_type="equals",
            value="Dupont"
        )
    ],
    path_to_file="dupont_users_with_threads.csv",
    n_threads=2
)

path_to_file_with_threads = full_data_resp_3.path_to_file

# Count number of items
n_items = bubble.count_items(
    bubble_type="user"
)

# Get object using its unique id
selected_user = bubble.get_object_by_id(
    bubble_type="user",
    unique_id="1655199935778x975916702499961900",
    columns_selected=["first_name", "last_name", "_id"]  # If not set, return all columns
)

# You can also get an object using another column
# You should only use columns with unique constraint
selected_order = bubble.get_object_by_id(
    bubble_type="order",
    column_id="ref",
    unique_id="100000",
    columns_selected=["ref", "_id"]  # If not set, return all columns
)

# You can decide to get a failure in case the result is not unique or if nothing is found
selected_dupont = bubble.get_object_by_id(
    bubble_type="user",
    column_id="last_name",
    unique_id="Dupont",
    fail_if_multiple_results=True,
    fail_if_not_found=True
)

# Create an object
order_unique_id = bubble.create_object(
    bubble_type="order",
    params={
        "ref": "100001"
    }
)

# Update an object
user_unique_id = bubble.update_object(
    bubble_type="user",
    params={
        "first_name": "Pierre"
    },
    unique_id="1655199935778x975916702499961900"
)

````
