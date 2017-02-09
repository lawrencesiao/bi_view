import os
import json
from app import app, db
from app.models import GameMeta, GameReplies, GameInning
from app.utils.utils import to_dict
from datetime import datetime
from flask import (
    abort, Blueprint, Response, request, render_template, jsonify
)
from flask.ext import excel
from flask.json import dumps
from jinja2 import TemplateNotFound
from sqlalchemy.sql import func


btv = Blueprint('btv', __name__)


@btv.route('/')
def index():
    return 'Hello from btv!'


@btv.route('/data/<filename>')
def data(filename):
    filepath = os.path.join(app.config['DATA_DIR'], 'btv', filename)
    def generate():
        with open(filepath, 'r') as f: 
            for line in f:
                yield line
    return Response(generate(), mimetype='text/csv')


@btv.route('/data/livestream/<date>')
def data_livestream(date):
    dirpath = os.path.join(
        app.config['DATA_DIR'], 'btv/{}/livestream'.format(date))
    def generate():
        for filename in os.listdir(dirpath):
            if filename.endswith('.csv'):
                fullpath = os.path.join(dirpath, filename)
                with open(fullpath, 'r') as f: 
                    for line in f:
                        yield line
    return Response(generate(), mimetype='text/csv')


@btv.route('/api/game_meta')
def api_game_meta():
    host_id = request.args['host_id']
    result = GameMeta.query.filter(
        host_id == host_id).order_by(GameMeta.start_time).all()
    return dumps([o.dict for o in result])


@btv.route('/api/game_inning.<ext>')
def api_game_inning(ext='json'):
    game_id = request.args['game_id']
    result = db.session.query(
        GameInning.order,
        func.convert_tz(
            GameInning.start_time, 'UTC', 'Asia/Taipei').label('start_time'),
        func.convert_tz(
            GameInning.end_time, 'UTC', 'Asia/Taipei').label('end_time'),
        GameInning.guest_score,
        GameInning.host_score).filter(GameInning.game_id == game_id).all()

    column_names = [
        'order', 'start_time', 'end_time', 'guest_score', 'host_score']

    if ext == 'csv':
        return excel.make_response_from_query_sets(
            result, column_names, 'csv', file_name='game_inning.csv')

    return dumps([to_dict(column_names, o) for o in result])


@btv.route('/api/game_replies.<ext>')
def api_replies(ext='json'):
    start_time = _timestamp_to_utcdatestring(request.args['start_time'])
    end_time = _timestamp_to_utcdatestring(request.args['end_time'])
    result = db.session.query(
        GameReplies.notation,
        GameReplies.topic,
        GameReplies.author,
        func.convert_tz(GameReplies.ts, 'UTC', 'Asia/Taipei').label('ts'),
        GameReplies.content).filter(db.and_(
            GameReplies.ts >= start_time, GameReplies.ts <= end_time)
        ).all()

    column_names = ['notation', 'topic', 'author', 'ts', 'content']
    if ext == 'csv':
        return excel.make_response_from_query_sets(
            result, column_names, 'csv', file_name='game_replies.csv')

    return dumps([to_dict(column_names, o) for o in result])


@btv.route('/<page>')
def show(page):
    try:
        return render_template('btv/%s.html' % page)
    except TemplateNotFound:
        abort(404)


def _timestamp_to_utcdatestring(ts):
    return datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S')
