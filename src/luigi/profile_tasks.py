# -*- coding: utf-8 -*-

import datetime
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from luigi.date_interval import Week, Month
from plugins import mysqldb
import action_tasks
import pandas as pd

class BaseProfileTask(config.BiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    table = "profile_user"
    columns = ["createdTS", "PK", "p_uid","p_mobilenumber", "p_emailaddress","p_name","p_birthday","p_gender","p_city","p_address","recency", "frequency", "monetary","first_source","first_action","first_target","last_source","last_action","last_target"]

    def rows(self):
        raise NotImplementedError("This method must be overridden")

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)
        return connection, cursor

# PYTHONPATH='' luigi --module profile_tasks InitiateProfile --local-scheduler
class InitiateProfile(BaseProfileTask):

    def run(self):
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        self.query = """
            UPDATE profile_user JOIN 
                (           SELECT F.user_id as user_id, R.r as recency , F.f as frequency, M.m as monetary ,fst.first_source , fst.first_action, fst.first_target ,R.last_source, R.last_action, R.last_target 
                FROM (SELECT tmp1.user_id, COUNT(*) AS f FROM (SELECT COUNT(*),timestamp,user_id ,source FROM log_action GROUP BY UNIX_TIMESTAMP(timestamp) DIV 21600, user_id ,source) tmp1 
                GROUP BY user_id) F 
                    INNER JOIN (SELECT user_id,sum(value) as m FROM log_action GROUP BY user_id) M on M.user_id = F.user_id 
                    INNER JOIN (SELECT user_id, source as last_source, action as last_action, target as last_target,MAX(timestamp) as r FROM log_action GROUP BY user_id ) R on R.user_id = F.user_id 
                    INNER JOIN (SELECT user_id, source as first_source, action as first_action, target as first_target FROM log_action GROUP BY user_id ) fst on fst.user_id = F.user_id) as tmp
            ON profile_user.p_mobilenumber = tmp.user_id
            SET profile_user.recency = tmp.recency,
                profile_user.frequency = tmp.frequency,
                profile_user.monetary = tmp.monetary,
                profile_user.last_source = tmp.last_source,
                profile_user.last_action = tmp.last_action,
                profile_user.last_target = tmp.last_target,
                profile_user.first_source = tmp.first_source,
                profile_user.first_action = tmp.first_action,
                profile_user.first_target = tmp.first_target
        """
        connection, cursor = self.connect(config.BiMySqlConfig) 

        connection.commit()

        connection.close()

#class DailyCreateProfile(BaseProfileTask):#

#    def requires(self):
#        return action_tasks.DailyAction(self.date)#

#    def connect(self, datasource):
#        connection = datasource().connection()
#        cursor = connection.cursor()
#        cursor.execute('SET NAMES UTF8')
#        cursor.execute(self.query)
#        connection.commit()#

#        return connection, cursor#

#    def rows(self):
#        self.query = """
#            SELECT F.user_id, R.r, F.f, M.m ,fst.first_source , fst.first_action, fst.first_target ,R.last_source, R.last_action, R.last_target 
#                FROM (SELECT tmp.user_id, COUNT(*) AS f FROM 
#                (SELECT COUNT(*),timestamp,user_id ,source 
#                    FROM (select * from log_action where user_id not in (select user_id from profile_user) and Date(timestamp) = '{date}') as log_action GROUP BY UNIX_TIMESTAMP(timestamp) DIV 21600, user_id ,source) tmp 
#                GROUP BY user_id) F 
#                    INNER JOIN (SELECT user_id,sum(value) as m FROM (select * from log_action where user_id not in (select user_id from profile_user) and Date(timestamp) = '{date}') as log_action GROUP BY user_id) M on M.user_id = F.user_id 
#                    INNER JOIN (SELECT user_id, source as last_source, action as last_action, target as last_target,MAX(timestamp) as r FROM (select * from log_action where user_id not in (select user_id from profile_user) and Date(timestamp) = '{date}') as log_action GROUP BY user_id ) R on R.user_id = F.user_id 
#                    INNER JOIN (SELECT user_id, source as first_source, action as first_action, target as first_target FROM (select * from log_action where user_id not in (select user_id from profile_user) and Date(timestamp) = '{date}') as log_action GROUP BY user_id ) fst on fst.user_id = F.user_id
#         """.format(date=self.date)
#        connection, cursor = self.connect(config.BiMySqlConfig) #

#        for row in cursor.fetchall():
#            yield row#

#        connection.close()


# PYTHONPATH='' luigi --module profile_tasks UpdateProfile --local-scheduler --date=2016-01-01
class UpdateProfile(BaseProfileTask):

    def requires(self):
        return (
            action_tasks.DailyAction(self.date),
            DailyAddUser(self.date)
            )

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        return connection, cursor

    def run(self):
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")
        
        self.query = """
            SELECT Min(timestamp),user_id,source,action,target from log_action where Date(timestamp) = '{date}' group by user_id
        """.format(date=self.date)

        connection_bi, cursor_bi = self.connect(config.BiMySqlConfig)
        cursor_bi.execute(self.query)


        connection = self.output().connect()
        cursor = connection.cursor()

        for row in cursor_bi.fetchall():
            _ ,user_id,source,action,target= row
            cursor.execute("""
                UPDATE profile_user
                set first_source = IF(first_source ='', '{source}',first_source),
                    first_action = IF(first_source ='', '{action}',first_action),
                    first_target = IF(first_source ='', '{target}',first_target)
                WHERE p_mobilenumber = '{user_id}'
                """.format(user_id=user_id,source=source,action=action,target=target))
            connection.commit()

        ## Update R, last action
        self.query = """
            UPDATE profile_user JOIN 
                (SELECT user_id, MAX(timestamp) as ts, source, action, target FROM log_action WHERE Date(timestamp) = '{date}' GROUP BY user_id) as tmp
            ON profile_user.p_mobilenumber = tmp.user_id
            SET profile_user.recency = tmp.ts,
                profile_user.last_source = tmp.source,
                profile_user.last_action = tmp.action,
                profile_user.last_target = tmp.target
                        """.format(date=self.date)

        cursor.execute(self.query)
        connection.commit()

        ## Update F, M
        self.query = """
                UPDATE profile_user JOIN 
                    (SELECT tmp.user_id, COUNT(*) as f, sum(tmp.value) as m
                        FROM (SELECT user_id, source, sum(value) AS value 
                            FROM log_action WHERE Date(timestamp) = '{date}' 
                            GROUP BY user_id, UNIX_TIMESTAMP(timestamp) DIV 21600) tmp 
                        GROUP BY tmp.user_id) toAdd
                ON profile_user.p_mobilenumber = toAdd.user_id
                SET profile_user.frequency = profile_user.frequency + toAdd.f,
                    profile_user.monetary = profile_user.monetary + toAdd.m
                    """.format(date=self.date)

        cursor.execute(self.query)
        connection.commit()

        ## Update user information from event
        self.query = """
        SELECT phone_number,email,real_name,gender,birthday,city,address 
        FROM registration_registerpbuser po 
        WHERE DATE(created_date) = '{date}'
        """.format(date=self.date)

        connection_event, cursor_event = self.connect(config.EventMySqlConfig)
        cursor_event.execute(self.query)


        for row in cursor_event.fetchall():
            phone_number, email, name, gender, birthday,city,address = row
            try:
                address=address.split(')')[1]
            except:
                pass

            cursor.execute("""
                UPDATE profile_user 
                SET p_emailaddress = "{email}" ,p_name = "{name}",p_gender="{gender}", p_birthday = "{birthday}" , p_city= "{city}" , p_address ="{address}"
                WHERE p_mobilenumber = '{phone_number}'
                """.format(email=email,name=name,gender=gender,birthday = birthday,city=city,address=address,phone_number=phone_number))
            connection.commit()

        ## Update user information from shop
        sql = """select p.user_login, MAX(CASE pm.meta_key WHEN 'shipping_address_1' THEN meta_value END) "address", MAX(CASE meta_key WHEN 'shipping_state' THEN meta_value END) "city", MAX(CASE meta_key WHEN 'billing_email' THEN meta_value END) "email",MAX(CASE meta_key WHEN 'billing_last_name' THEN meta_value END) "last_name", MAX(CASE meta_key WHEN 'billing_first_name' THEN meta_value END) "first_name"
        from wp_users as p inner join wp_usermeta as pm on p.ID = pm.user_id 
        where (pm.meta_key = 'shipping_address_1' or pm.meta_key = 'shipping_state' or pm.meta_key = 'billing_email' or pm.meta_key = 'billing_last_name' or pm.meta_key = 'billing_first_name') 
        and Date(p.user_registered) = '{date}' Group by p.ID
                    """.format(date=self.date) 

        con_shop, cursor_shop = self.connect(config.ShopMySqlConfig)

        df_shop = pd.read_sql(sql,con_shop)

        # combine first name and last name
        df_shop['name'] = [y+x if len(x) >= len(y) else x+y for x,y in zip(df_shop['last_name'],df_shop['first_name'])]     

        # eliminate those have name with phone type
        df_shop['name'] = [x if len(x) < 15 else '' for x in df_shop['name']]       

        for row in df_shop.iterrows():
            row=list(row[1])
            phone_number,address, city, email, _,_,name = row
            cursor.execute(
                """
                UPDATE profile_user 
                SET p_emailaddress = '{email}' ,p_name = '{name}', p_city= '{city}' , p_address ='{address}'
                WHERE p_mobilenumber = '{phone_number}'
                """.format(email=email,name=name,city=city,address=address,phone_number=phone_number))
            connection.commit()

        self.output().touch(connection)
        
        connection.commit()
        connection_event.commit()
        
        connection.close()
        connection_event.close()
        connection_bi.close()



# PYTHONPATH='' luigi --module profile_tasks DailyAddUser --local-scheduler --date=2016-01-01
class DailyAddUser(BaseProfileTask):

    def rows(self):
        self.query = """
            SELECT convert_tz(createdTS, 'Asia/Taipei','UTC'), PK, p_uid, p_mobilenumber, p_emailaddress,p_name,p_birthday,p_gender,'',p_domicileaddressdetail,'',0,0,'','','','','',''
            FROM users WHERE DATE(convert_tz(createdTS, 'Asia/Taipei','UTC')) = '{date}'
                """.format(date=self.date)

        connection, cursor = self.connect(config.SapMySqlConfig) 

        for row in cursor.fetchall():
            yield row

        connection.close()





