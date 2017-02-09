# -*- coding: utf-8 -*-
import datetime
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from luigi.date_interval import Week, Month
from plugins import mysqldb
import pandas as pd
import register_tasks


class BaseRegisterTask(config.ApiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "report_daily_register"
    columns = ["date", "source", "count"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor


# PYTHONPATH='' luigi --module register_tasks DailyEventRegister --local-scheduler --date=2016-01-01
#class DailyEventRegister(BaseRegisterTask):

#    def rows(self):
#        if self.date <= datetime.date(2016,05,22):
#            self.query = """
#                SELECT '{date}', '{source}', COUNT(*) FROM account_pbuser WHERE DATE(convert_tz(created_date, 'UTC', 'Asia/Taipei')) = '{date}'
#            """.format(date=self.date, source='event')
#            connection, cursor = self.connect(config.EventMySqlConfig)
#            for row in cursor.fetchall():
#                yield row
#
#            connection.close()
#
#        else:
#            pass

# PYTHONPATH='' luigi --module register_tasks DailyShopRegister --local-scheduler --date=2016-01-01
class DailyShopRegister(BaseRegisterTask):

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        df = pd.read_sql(self.query,connection)
        return connection, cursor, df

    def rows(self):

        self.query = """
            SELECT convert_tz(user_registered, 'UTC', 'Asia/Taipei') as ts, '{source}', user_login FROM wp_users WHERE DATE(convert_tz(user_registered, 'UTC', 'Asia/Taipei')) = '{date}'
        """.format(date=self.date,source='shop')

        connection, cursor, shop_df= self.connect(config.ShopMySqlConfig)

        connection.close()

        self.query = """
            select createdTS, p_mobilenumber from users where Date(createdTS) = '{date}' 
        """.format(date=self.date)

        connection, cursor, sap_df= self.connect(config.SapMySqlConfig)

        connection.close()


        sap_df['createdTS'] = [datetime.datetime.utcfromtimestamp(int(i/1e9)) for i in (sap_df['createdTS'].values.tolist())]

        bool_phone = shop_df['user_login'].isin(sap_df['p_mobilenumber'])

        boolean_pandas = [i[1]['ts']== sap_df[sap_df['p_mobilenumber']==i[1]['user_login']]['createdTS'] for i in shop_df[bool_phone.values].iterrows()]

        count = sum(([i.values[0] for i in boolean_pandas]))

        yield [self.date, 'shop', count]




class DailyMediaRegister(BaseRegisterTask):

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        df = pd.read_sql(self.query,connection)
        return connection, cursor, df

    def rows(self):

        self.query = """
            SELECT convert_tz(user_registered, 'UTC', 'Asia/Taipei') as ts, '{source}', user_login FROM wp_users WHERE DATE(convert_tz(user_registered, 'UTC', 'Asia/Taipei')) = '{date}'
        """.format(date=self.date,source='media')

        connection, cursor, mdedia_df= self.connect(config.MediaMySqlConfig)

        connection.close()

        self.query = """
            select createdTS, p_mobilenumber from users where Date(createdTS) = '{date}' 
        """.format(date=self.date)

        connection, cursor, sap_df= self.connect(config.SapMySqlConfig)

        connection.close()


        sap_df['createdTS'] = [datetime.datetime.utcfromtimestamp(int(i/1e9)) for i in (sap_df['createdTS'].values.tolist())]

        bool_phone = mdedia_df['user_login'].isin(sap_df['p_mobilenumber'])

        boolean_pandas = [i[1]['ts']== sap_df[sap_df['p_mobilenumber']==i[1]['user_login']]['createdTS'] for i in mdedia_df[bool_phone.values].iterrows()]

        count = sum(([i.values[0] for i in boolean_pandas]))

        print count

        yield [self.date, 'media', count]



class DailyAPIRegister(BaseRegisterTask):

    def rows(self):

        self.query = """
            SELECT '{date}', app_name , COUNT(*) 
            FROM api_app_log WHERE action ='Register' 
            AND comment = 'code:s - message:註冊成功，請用您的手機號碼和設定的密碼登入' 
            AND DATE(convert_tz(create_date, 'UTC', 'Asia/Taipei')) = '{date}' 
            GROUP BY app_name

         """.format(date= self.date)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyAllRegister(BaseRegisterTask):

    def rows(self):
        self.query = """
            SELECT '{date}', '{source}' ,count(*)
            FROM users WHERE DATE(createdTS) = '{date}'
                """.format(date=self.date,source='all')

        connection, cursor = self.connect(config.SapMySqlConfig) 

        for row in cursor.fetchall():
            print row
            print type(row)
            yield row

        connection.close()


# PYTHONPATH='' luigi --module register_tasks DailyRegister --local-scheduler --date=2016-01-01
class DailyRegister(BaseRegisterTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return (
            DailyMediaRegister(self.date),
            DailyAPIRegister(self.date),
            DailyShopRegister(self.date),
            DailyAllRegister(self.date)
        )

    def rows(self):
        # Notes: Dummy rows
        return []


# PYTHONPATH='' luigi --module register_tasks WeeklyRegister --local-scheduler --interval 2016-W19
class WeeklyRegister(BaseRegisterTask):

    interval = luigi.DateIntervalParameter(
        default=Week.from_date(datetime.date.today()))
    table = "report_weekly_register"
    columns = ["week", "source", "count", "start_date", "end_date"]

    def requires(self):
        return [DailyRegister(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{intervel}' AS week, source, SUM(count), '{str_date}', '{end_date}'
            FROM report_daily_register
            WHERE Date(date) >= '{str_date}' AND Date(date) < '{end_date}'
            GROUP BY source
        """.format(intervel=self.interval, str_date=self.interval.date_a, end_date=self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


# PYTHONPATH='' luigi --module register_tasks MonthlyRegister --local-scheduler --interval 2016-03
class MonthlyRegister(BaseRegisterTask):

    interval = luigi.DateIntervalParameter(
        default=Month.from_date(datetime.date.today()))
    table = "report_monthly_register"
    columns = ["month", "source", "count"]

    def requires(self):
        return [DailyRegister(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{interval}' AS month, source, SUM(count) AS count
            FROM report_daily_register WHERE DATE(date) >= '{str_date}' AND Date(date) < '{end_date}'
            GROUP BY source
        """.format(interval = self.interval, str_date= self.interval.date_a, end_date= self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()
