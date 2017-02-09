from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_object('config.production.Config')
db = SQLAlchemy(app)

from views.btv import btv
from views.example import example
app.register_blueprint(example, url_prefix='/example')
app.register_blueprint(btv, url_prefix='/btv')

# TODO:
# def create_app():
#     app = Flask(__name__)
#     app.config.from_object('config.production.Config')

#     from views.btv import btv
#     from views.example import example
#     app.register_blueprint(example, url_prefix='/example')
#     app.register_blueprint(btv, url_prefix='/btv')

#     return app
