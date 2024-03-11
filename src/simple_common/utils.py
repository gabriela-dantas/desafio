import sys
import importlib
import inspect
import simplejson as json

from boto3.dynamodb.types import Decimal
from typing import Type, List, Any, Dict

from simple_common.logger import logger


def get_all_class_instances(module_path: str) -> List[Any]:
    instances: List[object] = []

    class_members: List[tuple[str, Any]] = inspect.getmembers(
        sys.modules[module_path], inspect.isclass
    )

    for class_member in class_members:
        target_class: Type[object] = getattr(
            importlib.import_module(module_path), class_member[0]
        )

        instances.append(target_class())

    return instances


def set_lambda_response(status_code: int, body: Any) -> Dict[str, Any]:
    response = {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
    logger.info(f"Response: {response}")
    return response


def serialize_dynamo_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(item), parse_float=Decimal)


def deserialize_dynamo_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(item, use_decimal=True))
