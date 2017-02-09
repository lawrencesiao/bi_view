import IPython
from flask import *
from app import *
from termcolor import colored

app.testing = True
test_client = app.test_client()

welcome_message = """Welcome to your Flask CLI environment. 
The following variables are available to use:
app           -> Your Flask app instance.
test_client   -> Your Flask app.test_client().
"""

print colored(welcome_message, 'green')

IPython.start_ipython()
