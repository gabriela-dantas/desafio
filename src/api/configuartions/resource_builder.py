from fastapi_restful import Api
from typing import List

from api import resources
from api.resources.abstract_resource import AbstractResource
from simple_common.utils import get_all_class_instances


class ResourceBuilder:
    def __init__(self) -> None:
        self.__resources_module = resources.__name__

    def add_resources(self, api: Api) -> None:
        resource_instances: List[AbstractResource] = get_all_class_instances(
            self.__resources_module
        )

        for resource_object in resource_instances:
            api.add_resource(
                resource_object,
                resource_object.path(),
                tags=resource_object.tags(),
                dependencies=resource_object.dependencies,
            )
