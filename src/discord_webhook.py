import requests

def post_alert(url, message):
    payload = {
        "username":"collector",
        "content":message

    }
    requests.post(url, json=payload)
