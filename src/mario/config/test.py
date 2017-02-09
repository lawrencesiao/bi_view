from config import default


class Config(default.Config):

    TESTING = True
    SECRET_KEY = 'sekrit!'
