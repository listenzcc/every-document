'''
File: __init__.py
Author: Chuncheng Zhang
Purpose: Insert the parent.parent directory to the python path,
         so it finds the log folder.
'''
import sys
from pathlib import Path

# Insert the folder into the first element of the path array
sys.path.insert(0, Path(__file__).parent.parent)