# -*- coding: utf-8 -*-
import datetime
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from plugins import mysqldb
from luigi.date_interval import Week, Month


class BaseCancelTask(config.ShopMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "cancelled_post"
    columns = ["timestamp", "post_id", "source"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor



class DailyShopCancel(BaseCancelTask):

    def rows(self):
        self.query = """
            SELECT post_modified_gmt,ID, '{source}' 
            FROM wp_posts 
            WHERE DATE(convert_tz(post_modified_gmt, 'UTC', 'Asia/Taipei')) = '{date}' AND post_status = 'wc-cancelled'
        """.format(date=self.date, source='shop')

        connection, cursor = self.connect(config.ShopMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyCancel(BaseCancelTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return (
            DailyShopCancel(self.date)
        )

    def rows(self):
        # Notes: Dummy rows
        return []


class MonthlyCancel(BaseCancelTask):

    interval = luigi.DateIntervalParameter(
        default=Month.from_date(datetime.date.today()))

    def requires(self):
        return [DailyCancel(date) for date in self.interval]
