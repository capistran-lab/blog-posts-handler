import json
from typing import Dict, Any, Union
import service
from utils import build_response

def lambda_handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    """
    Main Router for the Blog Service.
    Handles HTTP events from API Gateway.
    """
    method = event.get('httpMethod', event.get('method', ''))
    params = event.get('pathParameters')
    # Handle empty or missing body
    body = json.loads(event.get('body', '{}')) if event.get('body') else {}

    try:
        if method == 'GET':
            if params and params.get('id'):
                post = service.get_post(params['id'])
                return build_response(200, post) if post else build_response(404, {"msg": "Post Not Found"})
            return build_response(200, service.list_posts())

        elif method == 'POST':
            post_id = service.create_post(body)
            return build_response(201, {"id": post_id, "message": "Created"})

        elif method == 'PATCH':
            if not params or not params.get('id'):
                return build_response(400, {"msg": "ID required"})
            service.update_post(params['id'], body)
            return build_response(200, {"id": params['id'], "message": "Updated"})

        elif method == 'DELETE':
            if not params or not params.get('id'):
                return build_response(400, {"msg": "ID required"})
            service.delete_post_soft(params['id'])
            return build_response(200, {"id": params['id'], "message": "Soft Deleted"})

        return build_response(405, {"msg": f"Method {method} not allowed"})

    except Exception as e:
        print(f"Server Error: {str(e)}")
        return build_response(500, {"msg": "Internal Server Error"})