import json
from header import Header

def payload_format(*args : str) -> str:
    payload = args[0]
    for i in range(1, len(args)):
        payload += "|" + args[i]
    return payload

def convert_from_json(data : dict) -> bytes:
    return json.dumps(data).encode("utf-8")

def convert_to_json(data: str) -> dict:
    return json.loads(data)

def to_bytes(data : str) -> bytes:
    return data.encode("utf-8")

def parse_payload(payload : str) -> tuple[Header, dict]:
    parse_payload = payload.decode("utf-8").split("|")
    header = Header(
        int(parse_payload[0]), 
        parse_payload[1], 
        parse_payload[2], 
        int(parse_payload[3])
    )
    data : dict
    if len(parse_payload) == 5:
        data = convert_to_json(parse_payload[4])
    else:
        data = None
    return header, data