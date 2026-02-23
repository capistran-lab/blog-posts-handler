import os
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, List, Any, Union

# Inicialización
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME', 'blog-website-table')
table = dynamodb.Table(TABLE_NAME)

def list_posts() -> List[Dict[str, Any]]:
    """Retrieve only items of type POST using the TypeIndex."""
    response = table.query(
        IndexName='TypeIndex',  # <-- Coincide con tu dynamo.tf
        KeyConditionExpression=Key('GSI1PK').eq('POSTS'),
        # Filtro opcional para soft-delete
        FilterExpression=Attr("is_deleted").ne(True),
        ScanIndexForward=False # Recientes primero (usa GSI1SK/fecha)
    )
    return response.get('Items', [])

def get_post(post_id: str) -> Union[Dict[str, Any], None]:
    """Retrieve using the Single Table PK/SK pattern."""
    response = table.get_item(
        Key={
            'PK': f"POST#{post_id}",
            'SK': "METADATA"
        }
    )
    item = response.get('Item')
    if not item or item.get('is_deleted') is True:
        return None
    return item

def get_post_by_slug(slug: str) -> Union[Dict[str, Any], None]:
    """Retrieve using the SlugIndex from your dynamo.tf."""
    response = table.query(
        IndexName='SlugIndex',
        KeyConditionExpression=Key('slug').eq(slug)
    )
    items = response.get('Items', [])
    return items[0] if items else None

def create_post(data: dict):
    # Aseguramos que el item tenga todas las llaves de tus índices
    item = {
        'PK': f"POST#{data['id']}",
        'SK': "METADATA",
        'id': data['id'],
        'title': data['title'],
        'slug': data['slug'],
        'content': data['content'],
        'author': data['author'],
        'date': data['date'],
        'tags': data['tags'],
        'imageUrl': data.get('imageUrl', '/board.png'),
        'GSI1PK': "POSTS",
        'GSI1SK': data['date'],
        'is_deleted': False
    }
    table.put_item(Item=item)
    return data['id']

def update_post(post_id: str, update_data: Dict[str, Any]) -> None:
    """Update using the correct PK/SK composite key."""
    expr = "SET "
    values = {}
    
    # No actualizamos las llaves primarias ni el ID
    safe_data = {k: v for k, v in update_data.items() if k not in ['PK', 'SK', 'id']}
    
    for key, value in safe_data.items():
        expr += f"{key} = :{key}, "
        values[f":{key}"] = value
    
    table.update_item(
        Key={
            'PK': f"POST#{post_id}",
            'SK': "METADATA"
        },
        UpdateExpression=expr.rstrip(", "),
        ExpressionAttributeValues=values
    )

def delete_post_soft(post_id: str) -> None:
    """Soft delete using PK/SK."""
    table.update_item(
        Key={
            'PK': f"POST#{post_id}",
            'SK': "METADATA"
        },
        UpdateExpression="SET is_deleted = :d, deleted_at = :t",
        ExpressionAttributeValues={
            ":d": True,
            ":t": datetime.datetime.utcnow().isoformat()
        }
    )