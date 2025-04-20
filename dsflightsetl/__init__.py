"""Init package"""

import logging
from os import getenv

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getenv("LOG_LEVEL", "DEBUG"))
