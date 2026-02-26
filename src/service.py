import os
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, List, Any, Union
from ulid import ULID # Necesitas instalar 'ulid-py'

# Inicialización
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME', 'blog-website-table')
table = dynamodb.Table(TABLE_NAME)

def list_posts() -> List[Dict[str, Any]]:
    response = table.query(
        IndexName='TypeIndex',
        KeyConditionExpression=Key('GSI1PK').eq('POSTS'),
        FilterExpression=Attr("is_deleted").ne(True),
        ScanIndexForward=False
    )
    items = response.get('Items', [])
    
    # Paracaídas: Aseguramos que authorId siempre exista al menos como string vacío
    for item in items:
        if 'authorId' not in item:
            item['authorId'] = "unknown"
            
    return items

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
    # 1. Generamos el ULID aquí en la Lambda
    # Usamos el que viene del body si existe (para curl), si no, uno nuevo.
    raw_id = data.get('id') or str(ULID())
    
    # 2. Construimos las llaves según tu patrón de Single Table
    # Si get_post usa "POST#{post_id}", aquí debemos guardar igual
    pk = f"POST#{raw_id}"
    sk = "METADATA"
    
    item = {
        'PK': pk,
        'SK': sk,
        'id': raw_id, # Guardamos el ID limpio para el Frontend
        'title': data['title'],
        'slug': data['slug'],
        'excerpt': data.get('excerpt', ''),
        'content': data['content'],
        'author': data['author'],
        'authorId': data.get('authorId', 'unknown'),
        'date': data['date'],
        'tags': data.get('tags', []),
        'imageUrl': data.get('imageUrl', '/board.png'),
        # Índices para listar (TypeIndex / GSI1)
        'GSI1PK': "POSTS",
        'GSI1SK': data['date'],
        'is_deleted': False
    }
    
    table.put_item(Item=item)
    return raw_id # Retornamos el ID generado

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