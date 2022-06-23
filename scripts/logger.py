import os,sys
sys.path.append(os.path.abspath(os.path.join('..')))
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

try:
    error_handler = logging.FileHandler('../logs/error.log')
except:
    try:
        error_handler = logging.FileHandler('logs/error.log')
    except Exception as e:
        print(e)

error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

try:
    info_handler = logging.FileHandler('../logs/access.log')
except:
    try:
        info_handler = logging.FileHandler('logs/access.log')
    except Exception as e:
        print(e)

info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(error_handler)
logger.addHandler(info_handler)
logger.addHandler(stream_handler)