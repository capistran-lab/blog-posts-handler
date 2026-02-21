import json
from typing import Dict, Any, Union
import service
from utils import build_response

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Router for the Blog Service.
    Handles HTTP events from API Gateway HTTP API (v2.0).
    """
    # FIX: Extract method from requestContext for HTTP API Payload 2.0
    # In v2.0, the method is located at event['requestContext']['http']['method']
    http_context = event.get('requestContext', {}).get('http', {})
    method = http_context.get('method', event.get('httpMethod', ''))
    
    params = event.get('pathParameters')
    
    # Handle empty or missing body safely
    body_raw = event.get('body')
    body = json.loads(body_raw) if body_raw else {}

    try:
        if method == 'GET':
            if params and params.get('id'):
                post = service.get_post(params['id'])
                return build_response(200, post) if post else build_response(404, {"msg": "Post Not Found"})
            
            return build_response(200, service.list_posts())

        elif method == 'POST':
            # This will handle the ULID and schema validation via service.py
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

        # If method is empty or not handled (like HEAD or OPTIONS)
        return build_response(405, {"msg": f"Method {method} not allowed"})

    except Exception as e:
        # Logging for CloudWatch debugging
        print(f"Server Error: {str(e)}")
        return build_response(500, {"msg": "Internal Server Error"})