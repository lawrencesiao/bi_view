from config import default


class Config(default.Config):

    DEBUG = True
    TESTING = False

    SQLALCHEMY_DATABASE_URI = 'mysql://root:54883155@dev-rds.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com/bi'
    # SQLALCHEMY_DATABASE_URI = 'mysql://root:54883155@dev-mysql/bi'
    SQLALCHEMY_ECHO = True
