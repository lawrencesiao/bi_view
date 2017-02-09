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


class BaseSalesTask(config.ApiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "report_daily_sales"
    columns = ["date", "source","item_id", "item_name", "sales", "amount", "comment"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor


# PYTHONPATH='' luigi --module sales_tasks QQ --local-scheduler --date=2016-04-25
class QQ(BaseSalesTask):

    def rows(self):

        if(pd.to_datetime(self.date) > pd.to_datetime('2016-04-24')):
            print 'yes'
        else:
            print 'fuck'

# PYTHONPATH='' luigi --module sales_tasks DailyShopSales --local-scheduler --date=2016-01-01
class DailyShopSales(BaseSalesTask):

    def rows(self):
        if(pd.to_datetime(self.date) < pd.to_datetime('2016-04-24')):

            self.query = """
                SELECT '{date}', '{source}', tmp2.item_id, tmp2.order_item_name, SUM(tmp2.sales), SUM(tmp2.qty), '{comment}'   
                FROM(
                    SELECT SUM(tmp.item_id) as item_id,tmp.order_item_id, tmp.order_item_name, SUM(sales) as sales, SUM(qty) as qty
                    FROM (
                        SELECT it.order_item_name,it.order_item_id, CASE meta_key WHEN '_line_total' THEN meta_value END "sales", CASE meta_key WHEN '_qty' THEN meta_value END "qty", CASE meta_key WHEN '_product_id' THEN meta_value END "item_id"
                        FROM wp_posts AS p
                        INNER JOIN wp_woocommerce_order_items AS it on p.ID = it.order_id
                        INNER JOIN wp_woocommerce_order_itemmeta AS itm on it.order_item_id = itm.order_item_id
                        WHERE (itm.meta_key = '_line_total' OR itm.meta_key = '_qty' OR itm.meta_key = '_product_id')
                            AND p.post_type = 'shop_order'
                            AND p.post_status = 'wc-completed'                        
                            AND it.order_item_id in (
                                SELECT order_item_id
                                FROM wp_woocommerce_order_items
                                WHERE order_id in (
                                    SELECT id FROM wp_posts
                                    WHERE DATE(post_date) = '{date}')
                                )
                    ) AS tmp
                    GROUP by tmp.order_item_id, tmp.order_item_name) AS tmp2 GROUP BY tmp2.item_id, tmp2.order_item_name
            """.format(date=self.date, source='shop', comment='')

        else:
            self.query = """
                SELECT '{date}', '{source}', tmp2.item_id, tmp2.order_item_name, SUM(tmp2.sales), SUM(tmp2.qty), '{comment}'   
                FROM(
                    SELECT SUM(tmp.item_id) as item_id,tmp.order_item_id, tmp.order_item_name, SUM(sales) as sales, SUM(qty) as qty
                    FROM (
                        SELECT it.order_item_name,it.order_item_id, CASE meta_key WHEN '_line_total' THEN meta_value END "sales", CASE meta_key WHEN '_qty' THEN meta_value END "qty", CASE meta_key WHEN '_product_id' THEN meta_value END "item_id"
                        FROM wp_posts AS p
                        INNER JOIN wp_woocommerce_order_items AS it on p.ID = it.order_id
                        INNER JOIN wp_woocommerce_order_itemmeta AS itm on it.order_item_id = itm.order_item_id
                        WHERE (itm.meta_key = '_line_total' OR itm.meta_key = '_qty' OR itm.meta_key = '_product_id')
                            AND p.post_type = 'shop_order'
                            AND it.order_item_id in (
                                SELECT order_item_id
                                FROM wp_woocommerce_order_items
                                WHERE order_id in (
                                  SELECT p.id FROM wp_posts AS p inner join wp_postmeta AS pm on p.id = pm.post_id
                                WHERE meta_key = '_paid_date' and Date(meta_value) = '{date}')
                                )
                    ) AS tmp
                    GROUP by tmp.order_item_id, tmp.order_item_name) AS tmp2 GROUP BY tmp2.item_id, tmp2.order_item_name
            """.format(date=self.date, source='shop', comment='')


        connection, cursor = self.connect(config.ShopMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyShopSalesRefund(BaseSalesTask):

    def rows(self):
        self.query = """
            SELECT '{date}', '{source}', tmp2.item_id, tmp2.order_item_name, SUM(tmp2.sales), -SUM(tmp2.qty), '{comment}'   
            FROM(
                SELECT SUM(tmp.item_id) as item_id,tmp.order_item_id, tmp.order_item_name, SUM(sales) as sales, SUM(qty) as qty
                FROM (
                    SELECT it.order_item_name,it.order_item_id, CASE meta_key WHEN '_line_total' THEN meta_value END "sales", CASE meta_key WHEN '_qty' THEN meta_value END "qty", CASE meta_key WHEN '_product_id' THEN meta_value END "item_id"
                    FROM wp_posts AS p
                    INNER JOIN wp_woocommerce_order_items AS it on p.ID = it.order_id
                    INNER JOIN wp_woocommerce_order_itemmeta AS itm on it.order_item_id = itm.order_item_id
                    WHERE (itm.meta_key = '_line_total' OR itm.meta_key = '_qty' OR itm.meta_key = '_product_id')
                        AND p.post_type = 'shop_order_refund'
                        AND it.order_item_id in (
                            SELECT order_item_id
                            FROM wp_woocommerce_order_items
                            WHERE order_id in (
                                SELECT id FROM wp_posts
                                WHERE DATE(post_date) = '{date}')
                            )
                ) AS tmp
                GROUP by tmp.order_item_id, tmp.order_item_name) AS tmp2 GROUP BY tmp2.item_id, tmp2.order_item_name
        """.format(date=self.date, source='shop', comment='refund')

        connection, cursor = self.connect(config.ShopMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()



#class DailyShopSalesCancelled(BaseSalesTask):

#    def rows(self):
#        # TODO: Removed duplicate code in DasilyShopSales.
#        self.query = """
#            SELECT '{date}', 'shop', tmp2.item_id, tmp2.order_item_name, -SUM(tmp2.sales), -SUM(tmp2.qty), '{comment}'   
#            FROM(
#                SELECT SUM(tmp.item_id) as item_id,tmp.order_item_id, tmp.order_item_name, SUM(sales) as sales, SUM(qty) as qty
#                FROM (
#                    SELECT it.order_item_name,it.order_item_id, CASE meta_key WHEN '_line_total' THEN meta_value END "sales", CASE meta_key WHEN '_qty' THEN meta_value END "qty", CASE meta_key WHEN '_product_id' THEN meta_value END "item_id"
#                    FROM wp_posts AS p
#                    INNER JOIN wp_woocommerce_order_items AS it on p.ID = it.order_id
#                    INNER JOIN wp_woocommerce_order_itemmeta AS itm on it.order_item_id = itm.order_item_id
#                    WHERE (itm.meta_key = '_line_total' OR itm.meta_key = '_qty' OR itm.meta_key = '_product_id')
#                        AND p.post_status = 'wc-cancelled'
#                        AND it.order_item_id in (
#                            SELECT order_item_id
#                            FROM wp_woocommerce_order_items
#                            WHERE order_id in (
#                                SELECT id FROM wp_posts
#                                WHERE DATE(convert_tz(post_date_gmt, 'UTC', 'Asia/Taipei')) = '{date}')
#                            )
#                ) AS tmp
#                GROUP by tmp.order_item_id, tmp.order_item_name) AS tmp2 GROUP BY tmp2.item_id, tmp2.order_item_name
#        """.format(date=self.date, source='shop', comment='cancel')

#        connection, cursor = self.connect(config.ShopMySqlConfig)

#        for row in cursor.fetchall():
#            yield row#

#        connection.close()


# PYTHONPATH='' luigi --module sales_tasks DailyEventSales --local-scheduler --date=2016-01-01
class DailyEventSales(BaseSalesTask):

    def rows(self):
        self.query = """
            SELECT '{date}', '{source}', rr.event_id, me.title, SUM(po.amount), COUNT(po.amount), '{comment}'
            FROM payment_order po 
            inner join registration_register rr on po.id = rr.order_id 
            inner join meal_event me on rr.event_id = me.id
            WHERE DATE(convert_tz(payment_date, 'UTC', 'Asia/Taipei')) = '{date}'
            GROUP BY me.title
            """.format(source='event',date=self.date,comment='')

        connection, cursor = self.connect(config.EventMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


# PYTHONPATH='' luigi --module sales_tasks DailyEventSalesRefund --local-scheduler --date=2016-01-01
class DailyEventSalesRefund(BaseSalesTask):

    def rows(self):
        self.query = """
            SELECT '{date}', '{source}', rr.event_id, me.title, -SUM(po.amount), -COUNT(po.amount), '{comment}'
            FROM payment_order po 
            inner join registration_register rr on po.id = rr.order_id 
            inner join meal_event me on rr.event_id = me.id
            WHERE DATE(convert_tz(po.updated_date, 'UTC', 'Asia/Taipei')) = '{date}'
            AND DATE(convert_tz(po.updated_date, 'UTC', 'Asia/Taipei')) > '2016-12-30'
            AND rr.status = 'orderCancelled'
            AND payment_date is not null
            GROUP BY me.title
            
            """.format(source='event',date=self.date,comment='refund')

        connection, cursor = self.connect(config.EventMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


# PYTHONPATH='' luigi --module sales_tasks DailySales --local-scheduler --date=2016-01-01
class DailySales(BaseSalesTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return (
            DailyShopSales(self.date),
            DailyShopSalesRefund(self.date),
            DailyEventSales(self.date),
            DailyEventSalesRefund(self.date)
        )

    def rows(self):
        # Notes: Dummy rows
        return []

# PYTHONPATH='' luigi --module sales_tasks WeeklySalesByItem --local-scheduler --interval 2016-W48
class WeeklySalesByItem(BaseSalesTask):

    interval = luigi.DateIntervalParameter(
        default=Week.from_date(datetime.date.today()))
    table = "report_weekly_sales_by_item"
    columns = ["week", "source", "item_id", "item_name", "sales", "amount", "start_day", "end_day", "comment"]

    def requires(self):
        return [DailySales(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{interval}', source, item_id, item_name ,SUM(sales), SUM(amount), '{str_date}', '{end_date}', comment
            FROM report_daily_sales 
            WHERE DATE(date) >= '{str_date}' AND DATE(date) < '{end_date}'
            GROUP BY source, item_id, item_name, comment
        """.format(interval=self.interval, str_date=self.interval.date_a, end_date=self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


class WeeklySales(BaseSalesTask):

    interval = luigi.DateIntervalParameter(
        default=Week.from_date(datetime.date.today()))

    table = "report_weekly_sales"
    columns = ["week", "source", "sales", "amount", "start_day", "end_day", "comment"]

    def requires(self):
        return WeeklySalesByItem(self.date, self.interval)

    def rows(self):
        self.query = """
            SELECT '{interval}', source ,SUM(sales), SUM(amount) ,'{str_date}', '{end_date}' , comment 
            FROM report_weekly_sales_by_item 
            WHERE week = '{interval}'
            GROUP BY source , comment
        """.format(interval=self.interval, str_date=self.interval.date_a, end_date=self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()


class MonthlySales(BaseSalesTask):

    interval = luigi.DateIntervalParameter(
        default=Month.from_date(datetime.date.today()))
    table = "report_monthly_sales"
    columns = ["month", "source", "sales", "amount", "comment"]

    def requires(self):
        return [DailySales(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{interval}' AS month, source, SUM(sales), SUM(amount), comment 
            FROM report_daily_sales WHERE DATE(date) BETWEEN '{str_date}' AND '{end_date}'
            GROUP BY source, comment
        """.format(interval = self.interval, str_date= self.interval.date_a, end_date= self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()

# PYTHONPATH='' luigi --module sales_tasks MonthlySalesByItem --local-scheduler
class MonthlySalesByItem(BaseSalesTask):

    interval = luigi.DateIntervalParameter(
        default=Week.from_date(datetime.date.today()))
    table = "report_monthly_sales_by_item"
    columns = ["month", "source", "item_id","item_name", "sales", "amount", "comment"]

    def requires(self):
        return [DailySales(date) for date in self.interval]

    def rows(self):
        self.query = """
            SELECT '{interval}', source, item_id, item_name ,SUM(sales), SUM(amount), comment
            FROM report_daily_sales WHERE DATE(date) BETWEEN '{str_date}' AND '{end_date}'
            GROUP BY source, item_id, comment
        """.format(interval=self.interval, str_date=self.interval.date_a, end_date=self.interval.date_b)

        connection, cursor = self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
            yield row

        connection.close()
