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
    
    path_params = event.get('pathParameters')
    query_params = event.get('queryStringParameters', {}) # <--- Add this
    
    # Handle empty or missing body safely
    body_raw = event.get('body')
    body = json.loads(body_raw) if body_raw else {}

    try:
        if method == 'GET':
            # Case 1: Search by ID in Path (/posts/{id})
            if path_params and path_params.get('id'):
                post = service.get_post(path_params['id'])
                return build_response(200, post) if post else build_response(404, {"msg": "Post Not Found"})
            
            # Case 2: Search by SLUG in Query String (/posts?slug=xyz)
            slug = query_params.get('slug') if query_params else None
            if slug:
                post = service.get_post_by_slug(slug)
                # We wrap it in 'items' to match your Zod schema in the frontend
                return build_response(200, {
                    "items": [post] if post else [],
                    "nextCursor": None
                })
            posts = service.list_posts()
            paginated_response = {
                                "items": posts,
                                "nextCursor": None  # Por ahora None hasta que implementes paginación real
                                }
            
        
            return build_response(200, paginated_response) 

        elif method == 'POST':
            # This will handle the ULID and schema validation via service.py
            post_id = service.create_post(body)
            return build_response(201, {"id": post_id, "message": "Created"})

        elif method == 'PATCH':
            if not path_params or not path_params.get('id'):
                return build_response(400, {"msg": "ID required"})
            
            service.update_post(path_params['id'], body)
            return build_response(200, {"id": path_params['id'], "message": "Updated"})

        elif method == 'DELETE':
            if not path_params or not path_params.get('id'):
                return build_response(400, {"msg": "ID required"})
            
            service.delete_post_soft(path_params['id'])
            return build_response(200, {"id": path_params['id'], "message": "Soft Deleted"})

        # If method is empty or not handled (like HEAD or OPTIONS)
        return build_response(405, {"msg": f"Method {method} not allowed"})

    except Exception as e:
        # Logging for CloudWatch debugging
        print(f"Server Error: {str(e)}")
        return build_response(500, {"msg": "Internal Server Error"})