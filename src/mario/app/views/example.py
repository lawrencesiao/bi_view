import os
from flask import current_app as app
from flask import Blueprint, Response, render_template

example = Blueprint('example', __name__, url_prefix='/example')


@example.route('/')
def index():
    return 'Hello World from example!'


@example.route('/data/<filename>')
def data(filename):
    filepath = os.path.join(app.config['DATA_DIR'], 'sample', filename)
    def generate():
        with open(filepath, 'r') as f: 
            for line in f:
                yield line
    return Response(generate(), mimetype='text/csv')


@example.route('/<page>')
def show(page):
    try:
        return render_template('example/%s.html' % page)
    except TemplateNotFound:
        abort(404)
