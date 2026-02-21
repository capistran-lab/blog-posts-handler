import json
from typing import Dict 

def lambda_handler(event: Dict, context: object) -> Dict:

    message: str = "Python Posts Crud - External Project"
   #cp print(json.dumps(event, indent=4))
    print(f"The event body is:{event.get('body')}")
    return {
        "statusCode":200,
        "body": json.dumps({
            "message": message,
            "status":"success"
        })
    }