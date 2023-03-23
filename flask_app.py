'''
File: flask_app.py
Author: Chuncheng Zhang
Purpose: Flask web service
'''

# %%
import flask
from pathlib import Path

# %%
html_root = Path(__file__).parent.joinpath('html')

# %%
app = flask.Flask(__name__)

# %%


@app.route('/')
def index():
    return open(html_root.joinpath('index.html')).read()


@app.route('/src/<path>')
def src(path):
    return open(html_root.joinpath('src', path), 'rb').read()


# %%
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7788)

# %%
