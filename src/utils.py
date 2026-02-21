import json
from typing import Any, Dict

def build_response(status_code: int, body: Any) -> Dict[str, Any]:
    """
    Standardize API response format.
    Includes CORS headers for Next.js compatibility.
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS"
        },
        "body": json.dumps(body)
    }