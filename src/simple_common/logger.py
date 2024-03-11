import logging
import os
import sys
import uuid


def create_logger() -> logging.Logger:
    app_name = os.environ["APP_NAME"]
    new_logger = logging.getLogger(app_name)

    level = os.environ.get("LOG_LEVEL", logging.INFO)
    new_logger.setLevel(level)
    new_logger.propagate = False
    if not new_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] - [%(name)s] - [%(levelname)s] - [%(module)s:%(lineno)d]"
                " - %(message)s - [%(data_info)s]"
            )
        )
        new_logger.addHandler(handler)

    return new_logger


log = create_logger()
extra_info = {"identifier": str(uuid.uuid1())}
logger = logging.LoggerAdapter(log, extra={"data_info": extra_info})
