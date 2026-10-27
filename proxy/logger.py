import logging
import sys

from settings import config


logger = logging.getLogger(__name__)
logger.setLevel(config.logging.level)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
