'''
File: flask_app.py
Author: Chuncheng Zhang
Purpose: Flask web service
'''

# %%
import os
import flask

from pathlib import Path
from every_document.every_document import EveryDocument

# %%
MAX_DEPTH = 3
ROOT_PATH = Path(os.environ['OneDriveCommercial'])
every_document = EveryDocument(ROOT_PATH, MAX_DEPTH)
every_document.convert_markdown()

# %%
html_root = Path(__file__).parent.joinpath('html')

# %%
app = flask.Flask(__name__)

# %%


def df2dict(df):
    df = df.copy()
    df['path'] = df['path'].map(lambda p: p.as_posix())
    return df.to_json()

# %%


@app.route('/')
def index():
    return open(html_root.joinpath('index.html')).read()


@app.route('/src/<path>')
def src(path):
    return open(html_root.joinpath('src', path), 'rb').read()


@app.route('/query/all-file')
def query_all_file():
    df = every_document.data_frame()
    return df2dict(df)


@app.route('/query/find-file')
def query_find_file():
    args = flask.request.args
    query = args.get('query', '')
    df = every_document.data_frame()
    df = df[df['name'].map(lambda name: query in name)]
    return df2dict(df)


@app.route('/query/find-markdown')
def query_find_markdown():
    args = flask.request.args
    query = args.get('query', '')
    df = every_document.df_with_markdown()
    df = df[df['markdown'].map(lambda name: query in name)]
    df['markdown'] = df['markdown'].map(lambda md: '\n'.join(
        [e for e in md.split('\n') if query in e]))
    df['path'] = df['path'].map(lambda p: p.relative_to(every_document.root))
    return df2dict(df)


# %%
if __name__ == '__main__':
    app.run(host='localhost', port=7788)

# %%
