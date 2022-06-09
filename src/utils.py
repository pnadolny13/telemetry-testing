import requests


def reset():
    resp = requests.get('http://localhost:9090/micro/reset')
    resp.raise_for_status()

def all():
    resp = requests.get('http://localhost:9090/micro/all')
    resp.raise_for_status()
    return resp.json()

def good():
    resp = requests.get('http://localhost:9090/micro/good')
    resp.raise_for_status()
    return resp.json()
