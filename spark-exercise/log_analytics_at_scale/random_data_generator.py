from datetime import datetime, timedelta
import pandas as pd
import random

# print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# timestamp, user_id, ip, endpoint, response_code, response_time


def random_timstamps(lines):
    timestamps = []
    base_timestamp = datetime.now()

    for i in range(lines):
        incremental_value = random.randint(1, 60)
        base_timestamp = base_timestamp + timedelta(seconds=incremental_value)
        new_timestmap = base_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        timestamps.append(new_timestmap)

    # print(timestamps)
    return timestamps


def generate_ip(lines):
    ip_addresses = []

    for i in range(lines):
        byte_one = str(random.randint(101, 200))
        byte_two = str(random.randint(101, 400))
        byte_three = str(random.randint(0, 99))
        byte_four = str(random.randint(0, 99))

        ip_address = ".".join([byte_one, byte_two, byte_three, byte_four])
        ip_addresses.append(ip_address)
    # print(ip_addresses)
    return ip_addresses


def generate_endpoint_and_resp_codes(lines):
    endpoints = []
    modules = {
        "/home": ["about", "services", "pricing", "documentation", "profile"],
        "/profile": ["details", "update", "view"],
        "/auth": ["login", "signin", "signout"],
        "/post": ["create", "edit", "delete", "get"]
    }

    response_codes = []
    response = {
        "/home": [200, 400, 500, 401, 403],
        "/profile": [200, 201, 204, 400, 500, 401, 403],
        "/auth": [200, 400, 500, 403],
        "/post": [200, 201, 400, 401, 500]
    }

    for i in range(lines):
        key = random.choice(list(modules))
        val = random.choice(modules[key])
        endpoint = "/".join([key, val])
        endpoints.append(endpoint)

        resp = random.choice(response[key])
        response_codes.append(resp)
    # print(response_codes)
    return endpoints, response_codes


def generate_response_time(lines):
    response_time = []
    for i in range(lines):
        response_time.append(random.randint(100, 500))
    # print(response_time)
    return response_time


def create_append_logs(lines):
    """timestamp, user_id, ip, endpoint, response_code, response_time"""
    endpoints, response_codes = generate_endpoint_and_resp_codes(lines)

    pd_dataframe = pd.DataFrame({
        "timestamp": random_timstamps(lines),
        "ip": generate_ip(lines),
        "endpoint": endpoints,
        "response_codes": response_codes,
        "response_time": generate_response_time(lines)
    })

    # print(pd_dataframe)
    pd_dataframe.to_csv("data/my_logs.csv", mode='a', index=False, header=not os.path.isfile("data/my_logs.csv"))


# random_timstamps(100)
# generate_ip(100)
# generate_endpoint_and_resp_codes(100)
# generate_response_time(100)
