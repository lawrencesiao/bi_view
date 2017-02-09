from . import db
import json


class GameMeta(db.Model):

    __tablename__ = 'game_meta'

    id = db.Column(db.Integer, primary_key=True)
    host_id = db.Column(db.String)
    guest_id = db.Column(db.String)
    host_name = db.Column(db.String)
    guest_name = db.Column(db.String)
    start_time = db.Column(db.TIMESTAMP)
    place = db.Column(db.String)

    @property
    def dict(self):
        return to_dict(self, self.__class__)


class GameInning(db.Model):

    __tablename__ = 'game_inning'

    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer)
    order = db.Column(db.Float)
    start_time = db.Column(db.TIMESTAMP)
    end_time = db.Column(db.TIMESTAMP)
    guest_score = db.Column(db.Integer)
    host_score = db.Column(db.Integer)

    @property
    def dict(self):
        return to_dict(self, self.__class__)


class GameReplies(db.Model):

    __tablename__ = 'game_replies'

    id = db.Column(db.Integer, primary_key=True)
    notation = db.Column(db.String)
    topic = db.Column(db.String)
    author = db.Column(db.String)
    content = db.Column(db.String)
    ts = db.Column(db.TIMESTAMP)

    @property
    def dict(self):
        return to_dict(self, self.__class__)


def to_dict(inst, cls):
    """
    Jsonify the sql alchemy query result.
    """
    convert = {
        'TIMESTAMP': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
        # db.TIMESTAMP(): lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
    }
    d = dict()
    for c in cls.__table__.columns:
        v = getattr(inst, c.name)
        if str(c.type) in convert.keys() and v is not None:
            try:
                d[c.name] = convert[str(c.type)](v)
            except:
                d[c.name] = "Error: Failed to covert using ", str(convert[c.type])
        elif v is None:
            d[c.name] = str()
        else:
            d[c.name] = v
    return d
