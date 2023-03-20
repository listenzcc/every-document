'''
File: logger.py
Author: Chuncheng zhang
Purpose: Provide logger for python script
'''

# %%
import logging
from . import PROJECT_NAME

# %%
dir(logging)
# %%
logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s %(message)s (line %(lineno)d, in %(module)s)')

LOGGER = logging.getLogger(PROJECT_NAME)
LOGGER.setLevel(logging.DEBUG)

# %%
if __name__ == '__main__':
    LOGGER.debug('Test debug')
    LOGGER.info('Test info')
    LOGGER.warning('Test warning')
    LOGGER.error('Test error')
    LOGGER.critical('Test critical')

# %%
