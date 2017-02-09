import os

class Config(object):

    BASE_DIR = os.path.dirname(os.path.realpath(os.path.dirname(__file__) + "/.."))
    DATA_DIR = os.path.join(BASE_DIR, 'luigi/data')

    DEBUG = True
    TESTING = False
    LANGUAGE_CODE = 'zh-TW'
    PORT = 8090

    MAX_CONTENT_LENGTH = 16 * 1024 * 1024    

    # Session settings
    SESSION_SECRET_KEY = 'pbplus54883155digital'

    # Token settings
    SECRET_KEY = '8^(9fg%o0#d0+ny$%*5u#obvk$86+(%_c2l)r+gjpu=@zm0vdj'
    EXPIRATION = 28800

    # # Database settings: DB_USER:DB_PASSWD@DB_HOST/DB_TABLE
    # DATABASES = {
    #     'sap': 'mysql://pbplus:pbplus@Systexdb.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com/pbplus?charset=utf8',
    #     'event': 'mysql://debut:tp6m4xup6@dev-rds.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com/event_develop?charset=utf8',
    #     'credit': 'mysql://debut:tp6m4xup6@dev-rds.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com/debut?charset=utf8',
    #     'debut': 'mysql://debut:tp6m4xup6@dev-rds.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com/debut?charset=utf8',
    # }

    # Email settings
    # MAIL_SERVER = 'smtp.gmail.com'
    # MAIL_PORT = 587
    # MAIL_USE_TLS = True
    # MAIL_USE_SSL = False
    # MAIL_USERNAME = 'pbplus.pcgbros@gmail.com'
    # MAIL_PASSWORD = 'qvhlqebofnqjifav'
    # MAIL_DEFAULT_SENDER = 'pbplus.pcgbros@gmail.com'
