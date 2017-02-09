# -*- coding: utf-8 -*-

import datetime
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from luigi.date_interval import Week, Month
from plugins import mysqldb


class BaseItemTask(config.ApiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "report_daily_item_released"
    columns = ["date", "source", "amount"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor


# PYTHONPATH='' luigi --module item_tasks DailyEventItem --local-scheduler --date=2016-01-01
class DailyEventItem(BaseItemTask):

    def rows(self):
        self.query = self.date.strftime("""
            SELECT '{date}', '{source}', count(*) 
            from (SELECT count(*)
            FROM meal_event 
            WHERE DATE(convert_tz(created_date, 'UTC', 'Asia/Taipei')) = '{date}'
            group by title) as tmp
        """).format(date= self.date,source='event')  
        connection, cursor = self.connect(config.EventMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

class DailyShopItem(BaseItemTask):

    def rows(self):

        self.query = self.date.strftime("""
            SELECT '{date}', '{source}',count(DISTINCT(post_parent)) 
            FROM wp_posts WHERE DATE(convert_tz(post_date, 'UTC', 'Asia/Taipei')) = '{date}' 
            AND post_status = 'publish' AND post_parent !=0        
            """).format(date= self.date, source= 'shop')
        connection, cursor = self.connect(config.ShopMySqlConfig) 
        
        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyItem(BaseItemTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return (
            DailyShopItem(self.date),
            DailyEventItem(self.date),
        )

    def rows(self):
        # Notes: Dummy rows
        return []

# PYTHONPATH='' luigi --module item_tasks WeeklyItem --local-scheduler --interval 2016-W30
class WeeklyItem(BaseItemTask):

    interval = luigi.DateIntervalParameter(
        default=Week.from_date(datetime.date.today()))

    table = "report_weekly_item"
    columns = ["week", "source", "amount", "start_date", "end_date"]

    def requires(self):
        return [DailyItem(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{date}', source , sum(amount), '{str_date}', '{end_date}'
            FROM report_daily_item_released
            WHERE DATE(date) >= '{str_date}' AND DATE(date) < '{end_date}'
            GROUP BY source 
        """.format(date= self.interval, str_date=self.interval.date_a, end_date=self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()

class MonthlyItem(BaseItemTask):

    interval = luigi.DateIntervalParameter(
        default=Month.from_date(datetime.date.today()))

    def requires(self):
        return [DailyItem(date) for date in self.interval]

