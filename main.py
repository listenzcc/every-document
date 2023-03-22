'''
File: main.py
Author: Chuncheng zhang
Purpose: Main function of the project
'''

# %%
import os

from pathlib import Path
from IPython.display import display
from every_document.every_document import EveryDocument

# %%
MAX_DEPTH = 3
# ROOT_PATH = Path(os.environ['OneDriveCommercial'])
ROOT_PATH = Path(r'C:\\Users\\zcc\\OneDrive - 中国科学院自动化研究所')

# %%
every_document = EveryDocument(ROOT_PATH, MAX_DEPTH)
every_document.convert_markdown()

# %%
helper = {
    'quit': 'Quit the program',
    'group-count': 'Group the DataFrame by suffix, display the counts',
    'group-first': 'Group the DataFrame by suffix, display the first record',
    'all-file': 'Display all the files',
    'find-file [string]': 'Find the files whose name contains the string',
    'all-markdown': 'Display all the files with markdown content',
    'find-markdown [string]': 'Display all the files with markdown content containing the string',
}

def unknown_input():
    '''
    Call it when the input is unknown.
    It will print the helper messages.
    '''
    print('\n----------------------------------------')
    print('- Commands')
    for key, value in helper.items():
        print('- {}: {}'.format(key, value))

    return

# %%

if __name__ == '__main__':
    while True:
        cmd = input('>> ')

        if cmd.startswith('quit'):
            break

        if cmd.startswith('group-count'):
            df = every_document.data_frame()
            group = df.groupby('suffix')
            display(group.count())
            continue

        if cmd.startswith('group-first'):
            df = every_document.data_frame()
            group = df.groupby('suffix')
            display(group.first())
            continue

        if cmd.startswith('all-file'):
            df = every_document.data_frame()
            display(df)
            continue

        if cmd.startswith('find-file '):
            split = [e.strip() for e in cmd.split(' ', 1) if e.strip()]
            if len(split) == 1:
                continue
            target = split[1]
            df = every_document.data_frame()
            select = df[df['name'].map(lambda name: target in name)]
            display(select)
            continue

        if cmd.startswith('all-markdown'):
            df_markdown = every_document.df_with_markdown()
            display(df_markdown)
            continue

        if cmd.startswith('find-markdown '):
            split = [e.strip() for e in cmd.split(' ', 1) if e.strip()]
            if len(split) == 1:
                continue
            target = split[1]
            df_markdown = every_document.df_with_markdown()
            select = df_markdown[df_markdown['markdown'].map(
                lambda name: target in name)]
            display(select)

            print('\n---- Detail ----')
            for j in select.index:
                split = [e.strip()
                         for e in select.loc[j, 'markdown'].split('\n')
                         if target in e]

                kwargs = dict(
                    idx=j,
                    name=select.loc[j, 'name'],
                    path=select.loc[j, 'path'].relative_to(
                        every_document.root),
                    content=split
                )
                print('{idx}: {name} {path}\n{content}\n'.format(**kwargs))
            continue

        unknown_input()

    print('ByeBye')

# %%
