'''
File: main.py
Author: Chuncheng zhang
Purpose: Main function of the project
'''

# %%
import os
import time
import pandas as pd

from pathlib import Path

from log.logger import LOGGER


# %%
MAX_DEPTH = 3

# %%
ROOT_PATH = Path(os.environ['OneDriveCommercial'])

# %%


def valid_dir(path):
    return all([
        path.is_dir(),
        not path.name.startswith('__pycache__'),
        not path.name.startswith('.')
    ])


def valid_file(path):
    return all([
        path.is_file(),
        not path.name.startswith('.'),
        '.' in path.name
    ])


class EveryDocument(object):
    '''
    Every Document object
    '''

    def __init__(self, root, max_depth=3):
        self.root = Path(root)
        self.max_depth = max_depth
        self._df = None
        self._df_need_update = True

        assert self.root.is_dir(), 'The root {} is not a directory'.format(self.root)

        pass

    def update(self):
        '''
        Prepare the empty array as the self.found,
        and fill it with files.
        '''
        self.found = []
        self.exceed_max_depth_times = 0

        t0 = time.time()
        self._dig(self.root, 0)
        self.update_cost_time = time.time() - t0

        self._df_need_update = True

        self.summary_latest()
        return self.found

    def summary_latest(self):
        '''
        Summary the latest update.
        '''
        self.latest_update_status = dict(
            found_files=len(self.found),
            max_depth=self.max_depth,
            exceed_max_depth_times=self.exceed_max_depth_times,
            update_cost_time=self.update_cost_time,
        )

        LOGGER.debug('Found {found_files} files in {update_cost_time} seconds, times of exceed max_depth ({max_depth}) is {exceed_max_depth_times}.'.format(
            **self.latest_update_status))

        if self.exceed_max_depth_times > 0:
            LOGGER.warning('The exceed_max_depth_times = {} is not Zero, so there are files not being processed.'.format(
                self.exceed_max_depth_times))

        return self.latest_update_status

    def data_frame(self):
        '''
        Get the DataFrame

        It normally returns the stored DataFrame object.
        When it is invalid or it needs being updated,
        it updates it automatically.
        '''
        if self._df is None or self._df_need_update:
            self._df = pd.DataFrame(self.found, columns=self._columns)
            LOGGER.info('The DataFrame is updated')

        return self._df

    def _mk_record(self, path, depth):
        '''
        Generate the column names,
        and the self._columns is also generated,
        to make sure the column names and their contents match with each other.
        '''
        self._columns = ['depth', 'suffix', 'name', 'lstat', 'path']
        return (depth, path.suffix.lower(), path.name, path.lstat(), path)

    def _dig(self, root, depth=0):
        '''
        Iterate through the root,
        to find all the files.

        The depth is accumulated until it exceeds the self.max_depth.

        The function is designed to be called iteratively.
        And the files are recorded into the self.found array.
        '''
        if depth > self.max_depth:
            self.exceed_max_depth_times += 1
            return

        LOGGER.debug('Dig ({}): {}'.format(depth, root))

        sub = list(root.iterdir())
        files = [self._mk_record(e, depth) for e in sub if valid_file(e)]
        dirs = [e for e in sub if valid_dir(e)]

        self.found += files

        [self._dig(p, depth+1) for p in dirs]


# %%
if __name__ == '__main__':
    every_document = EveryDocument(ROOT_PATH, MAX_DEPTH)
    every_document.update()
    df = every_document.data_frame()
    print('Bye bye')

# %%
group = df.groupby('suffix')
group.count()

# %%
group.first()

# %%
