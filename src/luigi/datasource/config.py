import luigi
import MySQLdb
from luigi.contrib.mysqldb import MySqlTarget

class MySqlConnection(luigi.Config):

    def connection(self):
        return MySQLdb.connect(
            host=self.host,
            user=self.user,
            passwd=self.password,
            db=self.database)

    def connection_url(self, driver='jdbc'):
        return '{}:mysql://{}:3306/{}?user={}&password={}'.format(
            driver, self.host, self.database, self.user, self.password)


class EventMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class ShopMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class MediaMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class ApiMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class CampaignMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class SapMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class BiMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()


class BiMySqlTarget(object):
    host = BiMySqlConfig().host
    database = BiMySqlConfig().database
    user = BiMySqlConfig().user
    password = BiMySqlConfig().password

class ShopMySqlTarget(object):
    host = ShopMySqlConfig().host
    database = ShopMySqlConfig().database
    user = ShopMySqlConfig().user
    password = ShopMySqlConfig().password

class ApiMySqlTarget(object):
    host = ApiMySqlConfig().host
    database = ApiMySqlConfig().database
    user = ApiMySqlConfig().user
    password = ApiMySqlConfig().password

class SapBiMySqlConfig(MySqlConnection):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()

    