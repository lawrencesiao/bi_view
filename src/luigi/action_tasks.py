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
from pandas.lib import Timestamp
import tempfile
import logging

logger = logging.getLogger('luigi-interface')


class BaseActionTask(config.BiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "log_action"
    columns = ["timestamp", "source", "user_id", "action", "target", "subcategory" ,"value", "qty"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor


# PYTHONPATH='' luigi --module action_tasks DailyActionShopBrowse --local-scheduler --date=2016-01-01
class DailyActionShopBrowse(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT convert_tz(from_unixtime(b.dt), 'Asia/Taipei','UTC'), '{source}', b.username,'{action}', a.post_title,'','','' 
            from wp_posts as a INNER JOIN wp_slim_stats as b on a.id = b.content_id 
            where DATE(convert_tz(from_unixtime(b.dt), 'Asia/Taipei','UTC'))='{date}' AND username LIKE '886%'
            """.format(date=self.date,source='shop',action='browse')  

        connection, cursor = self.connect(config.ShopMySqlConfig) 

        for row in cursor.fetchall():
           yield row

        connection.close()

# PYTHONPATH='' luigi --module action_tasks DailyActionShopBuy --local-scheduler --date=2016-01-01
class DailyActionShopBuy(BaseActionTask):

    def rows(self):
        self.query = """ 
            SELECT tmp2.post_date_gmt,'{source}', tmp2.user_login, '{action}', tmp2.order_item_name, tmp2.post_id, SUM(sales), SUM(qty)
            FROM (SELECT pd_table.post_date_gmt ,oi.order_item_name, u.user_login,tmp.post_id, CASE pd_table.meta_key WHEN '_line_total' THEN pd_table.meta_value END "sales", CASE pd_table.meta_key WHEN '_qty' THEN pd_table.meta_value END "qty" 
                FROM (SELECT * 
                    FROM `wp_postmeta` 
                    WHERE post_id in (SELECT ID 
                                    FROM `wp_posts` 
                                    WHERE Date(`post_date_gmt`)='{date}') AND `meta_key` = '_customer_user') tmp 
                                    INNER JOIN wp_users u on u.ID = tmp.meta_value 
                                    INNER JOIN wp_woocommerce_order_items oi on tmp.post_id = oi.order_id 
                                    INNER JOIN (SELECT p.post_date_gmt, oi.order_item_id,meta_key, meta_value 
                                                FROM `wp_woocommerce_order_itemmeta` oim 
                                                INNER JOIN wp_woocommerce_order_items oi on oi.order_item_id = oim.order_item_id 
                                                INNER JOIN wp_posts p on p.ID = oi.order_id 
                                                WHERE (`meta_key`= '_line_total' OR `meta_key`= '_qty') AND DATE(p.post_date_gmt)='{date}') pd_table on pd_table.order_item_id = oi.order_item_id 
                                                WHERE oi.order_item_type = 'line_item') tmp2
            GROUP BY post_date_gmt, order_item_name, user_login
        """.format(date=self.date, source='shop', action='buy')  
        connection, cursor = self.connect(config.ShopMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyActionShopCancel(BaseActionTask):

    def rows(self):
        self.query = """ 
            SELECT tmp2.post_modified_gmt,'{source}', tmp2.user_login, '{action}', tmp2.order_item_name, tmp2.post_id, -SUM(sales), -SUM(qty)
                        FROM (SELECT pd_table.post_modified_gmt ,oi.order_item_name, u.user_login,tmp.post_id, CASE pd_table.meta_key WHEN '_line_total' THEN pd_table.meta_value END "sales", CASE pd_table.meta_key WHEN '_qty' THEN pd_table.meta_value END 'qty' 
                            FROM (SELECT * 
                                FROM wp_postmeta 
                                WHERE post_id in (SELECT ID 
                                                FROM wp_posts 
                                                WHERE Date(post_modified_gmt)='{date}' AND post_status = 'wc-cancelled' AND ID not in (SELECT post_id FROM cancelled_post WHERE Date(timestamp) < '{date}')) AND meta_key = '_customer_user') tmp 
                                                INNER JOIN wp_users u on u.ID = tmp.meta_value 
                                                INNER JOIN wp_woocommerce_order_items oi on tmp.post_id = oi.order_id 
                                                INNER JOIN (SELECT p.post_modified_gmt, oi.order_item_id,meta_key, meta_value 
                                                            FROM wp_woocommerce_order_itemmeta oim 
                                                            INNER JOIN wp_woocommerce_order_items oi on oi.order_item_id = oim.order_item_id 
                                                            INNER JOIN wp_posts p on p.ID = oi.order_id 
                                                            WHERE (meta_key= '_line_total' OR meta_key= '_qty') AND DATE(p.post_modified_gmt)='{date}') pd_table on pd_table.order_item_id = oi.order_item_id 
                                                            WHERE oi.order_item_type = 'line_item'
                                                            ) tmp2
                        GROUP BY post_modified_gmt, order_item_name, user_login
        """.format(date=self.date, source='shop', action='cancel')  
        connection, cursor = self.connect(config.ShopMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

# PYTHONPATH='' luigi --module action_tasks DailyActionEventBrowse --local-scheduler --date=2016-01-01
class DailyActionEventBrowse(BaseActionTask):

#    def rows(self):
#        self.query = """
#            SELECT `ts`,'{source}',`username`,'{action}',`resource`,'','',''
#            FROM `log_pageview` 
#            WHERE `username` !='' AND DATE(ts)='{date}'
#        """.format(date=self.date,source='event',action='browse')  
#        connection, cursor = self.connect(config.EventMySqlConfig) #

#        for row in cursor.fetchall():
#            yield row#

#        connection.close()

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        return connection, cursor

    def run(self):
        con_event, cursor_event = self.connect(config.EventMySqlConfig) 
        sql = """SELECT lp.ts,'{source}',ab.phone_number,'{action}', lp.resource ,'','',''
                    FROM log_pageview as lp inner join account_pbuser as ab on ab.sap_id = lp.username
                    WHERE lp.username !='' AND DATE(convert_tz(lp.ts, 'UTC', 'Asia/Taipei')) ='{date}'
                    """.format(date=self.date,source='event',action='browse') 
        df_mysql = pd.read_sql(sql,con_event)       

        df_mysql['event_id'] = ''
        df_mysql['type'] = ''       
        

        # deal with mainpage
        df_mysql.ix[df_mysql['resource'] == '/','event_id'] = '0'
        df_mysql.ix[df_mysql['resource'] == '/','type'] = 'mainpage'        
        
        df_mysql.ix[df_mysql['resource'] == '/event/','event_id'] = '0'
        df_mysql.ix[df_mysql['resource'] == '/event/','type'] = 'mainpage'        


        # deal with those not belong mainpage
        non_mainpage_idx = [x and y for x, y in zip(df_mysql['resource'] != '/' ,df_mysql['resource'] != '/event/')]
        df_mysql.ix[non_mainpage_idx,'event_id'] = [text.split("/")[2] for text in df_mysql.ix[non_mainpage_idx,'resource']]
        df_mysql.ix[non_mainpage_idx,'type'] = [text.split("/")[-1] for text in df_mysql.ix[non_mainpage_idx,'resource']]     



#        df_mysql.ix[[x and y for x, y in zip(df_mysql['resource'] != '/' ,df_mysql['resource'] != '/event/')],'event_id'] = [text.split("/")[2] for text in df_mysql.ix[df_mysql['resource'] != '/','resource']]
 #       df_mysql.ix[[x and y for x, y in zip(df_mysql['resource'] != '/' ,df_mysql['resource'] != '/event/')],'type'] = [text.split("/")[-1] for text in df_mysql.ix[df_mysql['resource'] != '/','resource']]     

        df_mysql['event_id'] =df_mysql['event_id'].astype('int')


        df_event = pd.read_sql("SELECT id as event_id ,title FROM `meal_event` ",con_event)

        df_merged = pd.merge(df_mysql, df_event, on='event_id',how='left')
        df_merged.ix[pd.isnull(df_merged['title']),'title'] = '首頁'

        connection = self.output().connect()
        df_to_insert = df_merged[['ts','event','phone_number','browse','title','type']]
        df_to_insert.columns= ['timestamp','source','user_id','action','target','subcategory']

        df_to_insert['value'] = 0
        df_to_insert['qty'] = 0

        def to_the_second(ts):
            return Timestamp(long(round(ts.value, -9)))

        df_to_insert['timestamp'] = df_to_insert['timestamp'].apply(to_the_second)

        df_to_insert = df_to_insert.drop_duplicates()

        # write data into db
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        tmp_dir = luigi.configuration.get_config().get(
            'mysql', 'local-tmp-dir', None)
        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir)
        n = 0
        for row in df_to_insert.iterrows():
            row=list(row[1])
            n += 1
            if n % 100000 == 0:
                logger.info("Wrote %d lines", n)
            rowstr = self.column_separator.join(
                self.map_column(val) for val in row)
            rowstr += '\n'
            tmp_file.write(rowstr.decode('utf-8').encode('utf-8'))

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into mysql
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in xrange(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
                # self.post_copy(connection)
            except MySQLdb.ProgrammingError, e:
                if e[0] == errorcode.NO_SUCH_TABLE and \
                        attempt == 0:
                    # if first attempt fails with "relation not found", try
                    # creating table
                    logger.info("Creating table %s", self.table)
                    connection.rollback()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
        tmp_file.close()
        #df_to_insert.to_sql(name='log_action', con=connection, if_exists = 'append', index=False,flavor='mysql',chunksize=2000)

        # mark as complete in same transaction
        con_event.close()

class DailyActionEventPay(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT payment_date,'{source}',ap.phone_number,'{action}',trade_desc,item_name,item_price,''
            FROM `payment_order` po
            INNER JOIN account_pbuser ap on ap.`id`= po.pbuser_id
            WHERE DATE(payment_date)='{date}'
        """.format(date=self.date,source='event',action='pay')  
        connection, cursor = self.connect(config.EventMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()



class DailyActionEventRegister(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT po.created_date,'{source}',ap.phone_number,'{action}',po.trade_desc,item_name,0,''
            FROM payment_order po
            INNER JOIN account_pbuser ap on ap.id= po.pbuser_id
            WHERE DATE(po.created_date)='{date}'
        """.format(date=self.date,source='event',action='register')  
        connection, cursor = self.connect(config.EventMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()



class DailyActionNiceplay(BaseActionTask):

    def rows(self):

        self.query = """
            SELECT created_date,pc.event,CONCAT('886',SUBSTR(phone_number,2,10)),'poll',ps.description,'','',''
            FROM `cpbl_pollcount_below47` pc 
            INNER JOIN cpbl_pollsummary ps on ps.id =pc.v1 
            WHERE LENGTH(phone_number) = 10
            AND  Date(created_date) = '{date}'
            AND pc.event='{source}'
        """.format(date=self.date,source='niceplay',action='poll')  
        connection, cursor = self.connect(config.CampaignMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

        self.query = """
            SELECT created_date,pc.event,CONCAT('886',SUBSTR(phone_number,2,10)),'poll',ps.description,'','',''
            FROM `cpbl_pollcount_below329` pc 
            INNER JOIN cpbl_pollsummary ps on ps.id =pc.v1 
            WHERE LENGTH(phone_number) = 10
            AND Date(created_date) = '{date}'
            AND pc.event='{source}'
        """.format(date=self.date,source='niceplay',action='poll')  
        connection, cursor = self.connect(config.CampaignMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

        self.query = """
            SELECT created_date,pc.event,CONCAT('886',SUBSTR(phone_number,2,10)),'poll',ps.description,'','',''
            FROM `cpbl_pollcount` pc 
            INNER JOIN cpbl_pollsummary ps on ps.id =pc.v1 
            WHERE LENGTH(phone_number) = 10
            AND Date(created_date) = '{date}'
            AND pc.event='{source}'
        """.format(date=self.date,source='niceplay',action='poll')  
        connection, cursor = self.connect(config.CampaignMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyActionAllstar(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT created_date,pc.event,phone_number,'poll',ps.description,'','','' 
            FROM `cpbl_pollcount_below329` pc 
            INNER JOIN cpbl_pollsummary ps on ps.id =pc.v1 
            WHERE LENGTH(phone_number) = 12 AND pc.event='{source}' AND phone_number LIKE '886%' AND Date(created_date) = '{date}'
        """.format(date=self.date,source='allstar',action='poll')  
        connection, cursor = self.connect(config.CampaignMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


class DailyActiontaiwan_series_2016(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT created_date,pc.event,CONCAT('886',SUBSTR(phone_number,2,10)),'poll',ps.description,'','','' 
            FROM `cpbl_pollcount` pc 
            INNER JOIN cpbl_pollsummary ps on ps.id =pc.v1 
            WHERE LENGTH(phone_number) = 10 AND pc.event='taiwan_series_2016' 
        """.format(date=self.date,source='taiwan_series_2016',action='poll')  
        connection, cursor = self.connect(config.CampaignMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


class OneTimeActionSapBi(BaseActionTask):

    def rows(self):
        self.query = """
            SELECT convert_tz(ORDER_TIME, 'Asia/Taipei','UTC'), PROGRAM_NAME ,CONCAT('886',SUBSTR(MOBILE,2,10)),'buy',TICKET_TYPE, SESSION, PRICE ,1 
            FROM trans_raw1 
            WHERE ORDER_NUM IS NOT NULL AND MOBILE LIKE '09%'
        """
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

        self.query = """
            SELECT convert_tz(ORDER_TIME, 'Asia/Taipei','UTC'), PROGRAM_NAME , MOBILE,'buy',TICKET_TYPE, SESSION, PRICE ,1 
            FROM trans_raw1 
            WHERE ORDER_NUM IS NOT NULL AND MOBILE LIKE '8869%'
        """  
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

        self.query = """
            SELECT convert_tz(ORDER_TIME, 'Asia/Taipei','UTC'), PROGRAM_NAME , MOBILE,'buy',TICKET_TYPE, SESSION, PRICE ,1 
            FROM `TRANS_RAW`
            WHERE ORDER_NUM IS NOT NULL AND MOBILE LIKE '8869%'
        """  
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()

        self.query = """
            SELECT convert_tz(ORDER_TIME, 'Asia/Taipei','UTC'), PROGRAM_NAME , CONCAT('886',SUBSTR(MOBILE,2,10)),'buy',TICKET_TYPE, SESSION, PRICE ,1 
            FROM `TRANS_RAW`
            WHERE ORDER_NUM IS NOT NULL AND MOBILE LIKE '09%'
        """ 
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


        # 2016健達樂跑跑盃
        self.query = """
            SELECT STR_TO_DATE('20160501 0000','%Y%m%d %H%i'),PROGRAM_NAME,`MOBILE`,'register',"","","400","1" 
            FROM trans_raw1 WHERE PROGRAM_NAME='2016健達樂跑跑盃' AND MOBILE LIKE '8869%'
        """ 
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


        # 海賊王
        self.query = """
            SELECT STR_TO_DATE('20150517 0700','%Y%m%d %H%i'),PROGRAM_NAME,CONCAT('886',SUBSTR(MOBILE,2,10)),'register',"","","1400","1" 
            FROM trans_raw1 WHERE PROGRAM_NAME='2016航海王路跑' AND `MOBILE` LIKE '09%'
        """ 
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


        # 海賊王-高雄
        self.query = """
            SELECT STR_TO_DATE('20160131 0700','%Y%m%d %H%i'),PROGRAM_NAME,CONCAT('886',SUBSTR(MOBILE,2,10)),'register',"","","1280","1" 
            FROM trans_raw1 WHERE PROGRAM_NAME='2016_航海王路跑-高雄場' AND `MOBILE` LIKE '09%'
        """ 
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


        # 三商路跑
        self.query = """
            SELECT STR_TO_DATE('20151115 0700','%Y%m%d %H%i'),PROGRAM_NAME,CONCAT('886',SUBSTR(MOBILE,2,10)),'register',"","","900","1" 
            FROM trans_raw1 WHERE PROGRAM_NAME='三商路跑' AND `MOBILE` LIKE '09%'
        """ 
        connection, cursor = self.connect(config.SapBiMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()


        # TODO: 愛買盃


# PYTHONPATH='' luigi --module action_tasks DailyAction --local-scheduler --date=2016-01-01
class DailyAction(BaseActionTask):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return (
            DailyActionShopBuy(self.date),
            DailyActionShopBrowse(self.date),
            DailyActionEventPay(self.date),
            DailyActionEventBrowse(self.date),
            DailyActionShopCancel(self.date),
            DailyActionEventRegister(self.date),
            DailyActionNiceplay(self.date)
        #   DailyActionAllstar(self.date)
        )

    def rows(self):
        # Notes: Dummy rows
        return []


class MonthlyAction(BaseActionTask):

    interval = luigi.DateIntervalParameter(
        default=Month.from_date(datetime.date.today()))

    def requires(self):
        return [DailyAction(date) for date in self.interval]
        
    def rows(self):
        # Notes: Dummy rows
        return []



