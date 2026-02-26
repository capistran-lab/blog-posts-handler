import os
import boto3
import datetime

from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, List, Any, Union
 
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
    
    # safeguard
    for item in items:
        if 'authorId' not in item:
            item['authorId'] = "unknown"
        if 'id' not in item:
            item['id'] = item['PK'].replace('POST#', '')
            
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
 
    raw_id = data.get('id')
    
    pk = f"POST#{raw_id}"
    sk = "METADATA"
    
    item = {
        'PK': pk,
        'SK': sk,
        'id': raw_id,
        'title': data['title'],
        'slug': data['slug'],
        'excerpt': data.get('excerpt', ''),
        'content': data['content'],
        'author': data['author'],
        'authorId': data.get('authorId', 'unknown'),
        'date': data['date'],
        'tags': data.get('tags', []),
        'imageUrl': data.get('imageUrl', '/board.png'),
        'GSI1PK': "POSTS",
        'GSI1SK': data['date'],
        'is_deleted': False
    }
    
    table.put_item(Item=item)
    return raw_id

def update_post(post_id: str, update_data: Dict[str, Any]) -> None:
    """
    IMPORTANTE: El post_id que recibe debe ser el 'raw_id' (ej: 01H... )
    porque aquí le ponemos el prefijo POST#
    """
    expr = "SET "
    values = {}
    
    safe_data = {k: v for k, v in update_data.items() if k not in ['PK', 'SK', 'id', 'GSI1PK']}
    
    if 'date' in safe_data:
        safe_data['GSI1SK'] = safe_data['date']

    for key, value in safe_data.items():
        expr += f"#{key} = :{key}, "
        values[f":{key}"] = value
    

    names = {f"#{k}": k for k in safe_data.keys()}
    
    table.update_item(
        Key={
            'PK': f"POST#{post_id}",
            'SK': "METADATA"
        },
        UpdateExpression=expr.rstrip(", "),
        ExpressionAttributeValues=values,
        ExpressionAttributeNames=names
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