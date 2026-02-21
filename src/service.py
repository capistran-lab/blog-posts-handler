import os
import boto3
import datetime
from typing import Dict, List, Any, Union

# Global resource initialization
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME', 'blog-website-table')
table = dynamodb.Table(TABLE_NAME)

def list_posts() -> List[Dict[str, Any]]:
    """Retrieve all posts that are not soft-deleted."""
    response = table.scan(
        FilterExpression="attribute_not_exists(is_deleted) OR is_deleted = :f",
        ExpressionAttributeValues={":f": False}
    )
    return response.get('Items', [])

def get_post(post_id: str) -> Union[Dict[str, Any], None]:
    """Retrieve a single post if it exists and is not deleted."""
    response = table.get_item(Key={'id': post_id})
    item = response.get('Item')
    if not item or item.get('is_deleted') is True:
        return None
    return item

def create_post(data: Dict[str, Any]) -> str:
    """Initialize metadata and save new post."""
    data['created_at'] = datetime.datetime.utcnow().isoformat()
    data['is_deleted'] = False
    table.put_item(Item=data)
    return data['id']

def update_post(post_id: str, update_data: Dict[str, Any]) -> None:
    """Execute dynamic update expression for specific fields."""
    expr = "SET "
    values = {}
    for key, value in update_data.items():
        if key == 'id': continue
        expr += f"{key} = :{key}, "
        values[f":{key}"] = value
    
    table.update_item(
        Key={'id': post_id},
        UpdateExpression=expr.rstrip(", "),
        ExpressionAttributeValues=values
    )

def delete_post_soft(post_id: str) -> None:
    """Mark the post as deleted without removing the record."""
    table.update_item(
        Key={'id': post_id},
        UpdateExpression="SET is_deleted = :d, deleted_at = :t",
        ExpressionAttributeValues={
            ":d": True,
            ":t": datetime.datetime.utcnow().isoformat()
        }
    )