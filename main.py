'''
File: main.py
Author: Chuncheng zhang
Purpose: Main function of the project
'''

# %%
import os
import time
import pypandoc
import traceback
import threading
import pandas as pd

from pathlib import Path
from tqdm.auto import tqdm
from IPython.display import display

from log.logger import LOGGER


# %%
MAX_DEPTH = 3

# %%
PARALLEL_LIMIT = 10

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
        assert root.is_dir(), 'The root {} is not a directory, the EveryDocument can not start'.format(root)

        # Basic setup
        self.root = Path(root)
        self.max_depth = max_depth

        # Fast df
        # There are so many variables is because the process is fast.
        self._df = None
        self._df_needs_update_flag = True
        self.during_update_flag = False
        self.update_cost_time = -1

        # Slow df
        # It is a slow process so we just use it if it is available.
        self._df_with_markdown = None
        self.convert_markdown_cost_time = -1
        self.during_conversion_flag = False
        self.markdown_fail_files = []

        # Update at start
        # The function blocks the object initialization without being updated.
        self._update()

        pass

    def update(self):
        '''
        Update the df in the thread
        '''
        t = threading.Thread(target=self._update, daemon=True)
        t.start()
        LOGGER.debug('Update threading starts')
        return

    def _update(self):
        '''
        Prepare the empty array as the self.found,
        and fill it with files.
        '''
        self.during_update_flag = True
        self.found = []
        self.exceed_max_depth_times = 0

        t0 = time.time()
        self._dig(self.root, 0)
        self.update_cost_time = time.time() - t0

        self._df_needs_update_flag = True

        self.summary_latest()

        self.during_update_flag = False
        return self.found

    def convert_markdown(self):
        '''
        Convert to the markdown in the thread
        '''
        t = threading.Thread(target=self._convert_markdown, daemon=True)
        t.start()
        LOGGER.debug('Convert markdown threading starts')
        return

    def df_with_markdown(self):
        '''
        '''
        if self._df_with_markdown is None:
            LOGGER.error('The self._df_with_markdown is unavailable.')
            return

        if self.during_conversion_flag:
            LOGGER.warning(
                'The conversion is running, the results can be outdated')

        return self._df_with_markdown

    def _convert_markdown(self, allowed_suffix=['.docx'], parallel_limit=10):
        '''
        Fetch the markdown content from the files
        '''
        self.during_conversion_flag = True
        t0 = time.time()

        # Request the latest DataFrame
        df = self.data_frame()

        # Filter by the allowed_suffix
        df = df[df['suffix'].map(lambda s: any(
            [e in s for e in allowed_suffix]))]

        # Convert the documents to the markdown format
        jobs = []
        markdown_collection = []
        success_files = []
        fail_files = []

        def convert(path):
            '''
            Convert the file of path to markdown string,
            and the converted file is stored in the markdown_collection array,
            the columns are ['path', 'markdown'].

            And the files of success & fail are stored in the success_files and fail_files array.
            '''

            # print(path)

            try:
                markdown = pypandoc.convert_file(path, 'markdown')
                markdown_collection.append((path, markdown))
                success_files.append(path)
                return markdown
            except Exception as e:
                fail_files.append(path)
                traceback.print_exc()
            return

        def operate_jobs(force_operate=False):
            '''
            Operate the jobs in parallel threads,
            the operations are only called when either force_operate,
            or the jobs are more then parallel_limit.
            '''
            if force_operate or len(jobs) >= parallel_limit:
                # Start the jobs
                [job.start() for job in jobs]

                # Join the jobs, until they are done
                while len(jobs) > 0:
                    job = jobs.pop()
                    job.join()

            return

        for j in tqdm(df.index, 'Converting to markdown'):
            path = df.loc[j, 'path']
            t = threading.Thread(target=convert, args=(path,), daemon=True)
            jobs.append(t)
            # Finish the jobs if the conditions are matched.
            operate_jobs()

        # Finish the remain jobs
        operate_jobs(force_operate=True)

        # Build the DataFrame of the path & markdown
        df_with_markdown = pd.DataFrame(
            markdown_collection, columns=['path', 'markdown'])

        # Merge into the DataFrame
        df_with_markdown = pd.merge(
            df, df_with_markdown, on='path', how='left')
        df_with_markdown.fillna('', inplace=True)

        self._df_with_markdown = df_with_markdown
        self.markdown_fail_files = fail_files
        self.convert_markdown_cost_time = time.time() - t0

        self.during_conversion_flag = False

        return df_with_markdown

    def summary_latest(self):
        '''
        Summary the latest update.
        '''
        self.latest_running_status = dict(
            found_files=len(self.found),
            max_depth=self.max_depth,
            exceed_max_depth_times=self.exceed_max_depth_times,
            update_cost_time=self.update_cost_time,
            convert_markdown_cost_time=self.convert_markdown_cost_time,
            during_update_flag=self.during_update_flag,
            during_conversion_flag=self.during_conversion_flag,
            markdown_fail_files=self.markdown_fail_files,
        )

        LOGGER.debug('Found {found_files} files in {update_cost_time} seconds, times of exceed max_depth ({max_depth}) is {exceed_max_depth_times}.'.format(
            **self.latest_running_status))

        LOGGER.debug('Latest summary is {}'.format(self.latest_running_status))

        if self.exceed_max_depth_times > 0:
            LOGGER.warning('The exceed_max_depth_times = {} IS NOT 0, so there are files not being processed.'.format(
                self.exceed_max_depth_times))

        return self.latest_running_status

    def data_frame(self):
        '''
        Get the DataFrame

        It normally returns the stored DataFrame object.
        When it is invalid or it needs being updated,
        it updates it automatically.

        !!! Keep it in mind that the self.found may be incomplete,
        !!! since it is dynamically updated during self.update()
        '''
        if self.during_update_flag:
            LOGGER.warning(
                'The update process is running, the results is incomplete')

        if self._df is None or self._df_needs_update_flag:
            # Generate the new self._df
            self._df = pd.DataFrame(self.found, columns=self._columns)

            # Toggle the self._df_needs_update flag
            self._df_needs_update_flag = False
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
every_document = EveryDocument(ROOT_PATH, MAX_DEPTH)

# %%
df = every_document.data_frame()
group = df.groupby('suffix')
display(group.count())
display(group.first())
display(df)

# %%
every_document._convert_markdown()

# %%
every_document.df_with_markdown()

# %%
every_document.summary_latest()

# %%


# %%

def deprecated_function():
    jobs = []
    markdown_collection = []
    success_files = []
    fail_files = []

    def convert(path):
        '''
        Convert the file of path to markdown string,
        and the converted file is stored in the markdown_collection array,
        the columns are ['path', 'markdown'].

        And the files of success & fail are stored in the success_files and fail_files array.
        '''

        # print(path)

        try:
            markdown = pypandoc.convert_file(path, 'markdown')
            markdown_collection.append((path, markdown))
            success_files.append(path)
            return markdown
        except Exception as e:
            fail_files.append(path)
            traceback.print_exc()
        return

    def operate_jobs(force_operate=False):
        '''
        Operate the jobs in parallel threads,
        the operations are only called when either force_operate,
        or the jobs are more then PARALLEL_LIMIT.
        '''
        if force_operate or len(jobs) >= PARALLEL_LIMIT:
            # Start the jobs
            [job.start() for job in jobs]

            # Join the jobs, until they are done
            while len(jobs) > 0:
                job = jobs.pop()
                job.join()

        return

    for j in tqdm(df.index, 'Converting to markdown'):
        path = df.loc[j, 'path']
        t = threading.Thread(target=convert, args=(path,), daemon=True)
        jobs.append(t)
        # Finish the jobs if the conditions are matched.
        operate_jobs()

    # Finish the remain jobs
    operate_jobs(force_operate=True)

    # Build the DataFrame of the path & markdown
    df_with_markdown = pd.DataFrame(
        markdown_collection, columns=['path', 'markdown'])

    # Merge into the DataFrame
    df_with_markdown = pd.merge(df, df_with_markdown, on='path', how='left')
    df_with_markdown.fillna('', inplace=True)
    df_with_markdown

    # %%
    df_with_markdown[df_with_markdown['markdown'].map(lambda x: '决算' in x)]

# %%
