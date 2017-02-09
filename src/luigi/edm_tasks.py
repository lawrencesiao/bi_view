# -*- coding: utf-8 -*-
import datetime
import MySQLdb
import requests
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from luigi.date_interval import Week, Month
from plugins import mysqldb
import pandas as pd
import jinja2
from jinja2 import Template
import os



# PYTHONPATH='' luigi --module edm_tasks DailyEdmRun --local-scheduler
class DailyEdmRun(luigi.Task):

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)

        return connection, cursor

    def run(self):
    	program = 1
    	batch = 2
        self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 700
        """.format(program=program,batch=batch)

        connection, cursor= self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
			hash_ ,mail = row 
			message = """
			<html>
			<body>			

			<center>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/27/info&hash={hash}&type=click_url_info&program={program}&batch={batch}&source=edm">
			<h2>更多資訊</h2>
			</a>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/27/signup&hash={hash}&type=click_url_signup&program={program}&batch={batch}&source=edm">
			<h2>立即報名</h2>
			</a>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/27/info&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">
			<img src="https://api.pbplus.me/track/redirect?url=https://s3-ap-northeast-1.amazonaws.com/pbplus-event/event_img/edm/mli_run.jpg&hash={hash}&type=load_img&program={program}&batch={batch}&source=edm
			" alt="mli_run">
			</a>
			</center>
			</body>
			</html>
				""".format(hash=hash_,program=program,batch=batch)		

			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'三商公益路跑，越跑越有•愛','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))

			cursor.connection.commit()
        
        connection.close()

# PYTHONPATH='' luigi --module edm_tasks DailyEdmSpartan --local-scheduler
class DailyEdmSpartan(luigi.Task):

    def connect(self, datasource):
        connection = datasource().connection()
        cursor = connection.cursor()
        cursor.execute('SET NAMES UTF8')
        cursor.execute(self.query)

        return connection, cursor

    def run(self):
    	program = 2
    	batch = 1
        self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 300
        """.format(program=program,batch=batch)

        connection, cursor= self.connect(config.ApiMySqlConfig)

        for row in cursor.fetchall():
			hash_ ,mail = row 
			message = """
			<html>
			<body>			
			親愛的Spartan Race 勇士您好！<br>
			<br>
			<br>
			全球障礙路跑領導品牌Spartan Race美國媒體NBC評為最佳障礙路跑賽事品牌，恭喜您報名成功！<br>
			<br>
			2015年包括美國、英國、澳洲，加拿大、德國、法國、韓國等超過28個國家曾舉辦過Spartan Race，平均每個國家參賽人數5,000~20,000人次左右，總計全球超越45萬人次曾經參與競賽。每個國家都會設計表現各國文化的紀念Ｔ恤，最能代表台灣人的拚與勇精神就是台灣黑熊，袖口上印製象徵台灣的台字形象，更能讓人一眼就看出這是屬於台灣的斯巴達紀念Ｔ恤。<br>
			<br>
			加價購NT$599（數量有限，為維護權益，每個帳號限購一件，團體報名請團員用自己的帳號各自訂購～）<br>

			<a href="https://api.pbplus.me/track/redirect?url=https://goo.gl/Gx6d0U&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">
			<img src="https://api.pbplus.me/track/redirect?url=https://s3-ap-northeast-1.amazonaws.com/pbplus-event/event_img/edm/Spartan-TEE.jpg&hash={hash}&type=load_img&program={program}&batch={batch}&source=edm
			" alt="mli_run" width="700" height="302">
			</a>
			<br>
			請注意，此加購紀念Ｔ恤需於2016台灣斯巴達障礙路跑現場領取，務必確認您填寫的身分證字號/護照號碼正確無誤，未到場領取或是非賽事跑者恕不補發及退費哦～<br>
			<br>
			<font color="red">數量有限，售完不補！火熱選購連結：</font><a href="https://api.pbplus.me/track/redirect?url=https://goo.gl/Gx6d0U&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">點我搶購去！</a>
			<br>
			<br>
			<strong>一起為台灣Aroo~~~~</strong>
			</body>
			</html>
				""".format(hash=hash_,program=program,batch=batch)		

			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'限量加購！2016 Spartan Race Taiwan台灣斯巴達障礙路跑紀念T恤','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))

			cursor.connection.commit()
        
        connection.close()

# PYTHONPATH='' luigi --module edm_tasks DailyImportRegisterEvent60 --local-scheduler
class DailyImportRegisterEvent60(luigi.Task):
	date = luigi.DateParameter(default=datetime.date.today())

	def run(self):

		db= MySQLdb.connect(host='media-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com', 
		                user='media_pbplus_me', passwd='pcgbros_54883155', 
		                db='event')		

		cursor = db.cursor()		

		db_api= MySQLdb.connect(host='media-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com', 
		                user='media_pbplus_me', passwd='pcgbros_54883155', 
		                db='api')		

		cursor_api = db_api.cursor()		
		

		cursor_api.execute("select hash from log_edm_response where program_id=2 and batch_id=1 order by id desc limit 1")
		j = cursor_api.fetchall()
		j = int(j[0][0])		

		df = pd.read_sql("""
			select rri.value,rrd.team_max_select 
			from registration_register as rr inner join registration_registeritem as rri on rr.id = rri.register_id and rr.event_id = 59 
			inner join registration_registerdish as rrd on rrd.register_id = rr.id where rr.status = 'paymentCleared' and rr.updated_date >= subdate(current_date, 2)
								""",con=db)		

		index = [i for i in xrange(df.shape[0])]		

		#cols = ['name', 'Last_name','first_name','gender','social_id','birth','phone','email','size','contacter','number_contacter']
		cols=['hash','email','response','program_id','batch_id']
		final = pd.DataFrame(columns=cols)		

		for i in index:
			city=df.ix[i,0]
			print df.ix[i,0]
			for person in df.ix[i,0].split("+_)(*&^%$#@!"):
				if len(person.split("!@#$%^&*()_+")) !=11:
					pass
				else:
					j+=1
					hash_ = str(60)+str(j)
					program_id=2
					batch_id=1
					response=''
					_,_,_,_,_,_,phone,email,_,_,_ = person.split("!@#$%^&*()_+")
					tmp=[hash_,email,response,program_id,batch_id]
					tmp_df = pd.Series(tmp,index=cols)
					final = final.append(tmp_df,ignore_index=True)		
		
		#final.to_sql(con=db_api, name='log_edm_response', if_exists='append', flavor='mysql',index=False)		
		

		#df is a dataframe
		num_rows = len(final)
		#Iterate one row at a time
		for i in range(num_rows):
			try:
		 	#Try inserting the row
		 		final.iloc[i:i+1].to_sql(name="log_edm_response",con = db_api,if_exists = 'append',index=False,flavor='mysql')
			except:
			#Ignore duplicates
				pass

## huahua
# PYTHONPATH='' luigi --module edm_tasks DailyEdmHuaHua --local-scheduler
class DailyEdmHuaHua(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def run(self):
		program = 3
		batch = self.batch
#    	batch = 1
		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 200
		""".format(program=program,batch=batch)

		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 
			message = """
			<html>
			<body>
			<br>
			六年、超過兩千個打數、突破三千個打席。中信兄弟開路先鋒 : 張正偉。<br>
 			<br>
			青少棒、青棒、成棒階段都曾因外在條件影響，而萌生放下球棒的想法。阿美族的他，帶著部落樂觀、大方的天性，加上本身對於棒球堅持不息的信念，讓花花格外珍惜自己的棒球生命。<br>
 			<br>
			即便適逢低潮、或在球隊人手不足時，全勤扛住球隊供輸要角。「我熱愛棒球」，短短一句話，加上總是潔白的笑顏，張正偉將珍惜與熱愛實踐地淋漓盡致。<br>
 			<br>
			現在讓我們一同期待，這株綻放在右外野草皮的溫柔之花，即將達陣選手生涯地重要里程碑！<br>
 			<br>
 			<br>
			花花－張正偉，千安，永不息！讓我們一起見證歷史。<br>
 			<br>
			【限量珍藏】張正偉千安紀念套組（紀念棒+紀念T)<br>
			<br>
			定價: NT$1,680      折扣價NT$1,360<br>
			<br>
			<font color="red">限量珍藏，組合有限 !火熱點購:  </font><a href="https://api.pbplus.me/track/redirect?url=http://goo.gl/RZ9yj9&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">點我搶購去！</a>
			<br>
			<br>

			<a href="https://api.pbplus.me/track/redirect?url=http://goo.gl/RZ9yj9&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">
			<img src="https://api.pbplus.me/track/redirect?url=https://s3-ap-northeast-1.amazonaws.com/pbplus-event/event_img/edm/huahua.jpg&hash={hash}&type=load_img&program={program}&batch={batch}&source=edm
			" alt="huahua" width="500" >
			</a>
			<br>
			</body>
			</html>
				""".format(hash=hash_,program=program,batch=batch)		

			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'【獨家限量】萬眾期待，綻放千安！張正偉千安紀念套組發售中！','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))

			cursor.connection.commit()
        
		connection.close()

# PYTHONPATH='' luigi --module edm_tasks DailyUpdateEdmActionNumber --local-scheduler
class DailyUpdateEdmActionNumber(luigi.Task):

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		return connection, cursor

	def event_action_calculate(self,program_id,source,batch_id,target,con_event,cursor_event,con_api,cursor_api):	

		event_id = target
		register_action_df = pd.read_sql("""
			select rr.created_date as action_date,rrp.email
			from registration_register as rr inner join registration_registerpbuser as rrp on rrp.register_id = rr.id 
			where event_id = '{event_id}'
		""".format(event_id=event_id), con=con_event)		
		## keep the max one
		idx = register_action_df.groupby(['email'])['action_date'].transform(max) == register_action_df['action_date']
		register_action_df = register_action_df.ix[idx,:]
		## remove duplicate

		register_action_df = register_action_df.drop_duplicates(take_last=True)	

		register_send_df = pd.read_sql("""
			select updated_date as send_date ,email from log_edm_response where response = '<Response [200]>' and program_id='{program_id}' and batch_id='{batch_id}'
			""".format(program_id=program_id,batch_id=batch_id),con=con_api)		


		all_ = pd.merge(register_send_df, register_action_df, 
		                            how='left',
		                            left_on=['email'], 
		                            right_on=['email'],
		                            left_index=False, right_index=False, sort=True, copy=False) 	
			

		number_action = sum(all_.send_date < all_.action_date)	

		cursor_api.execute("""
		    UPDATE edm_list
		    set number_action = '{number_action}'
		    WHERE program_id = '{program_id}' and batch_id = '{batch_id}' and source='{source}' and target='{target}'
		    """.format(number_action=number_action,program_id=program_id,batch_id=batch_id,source=source,target=target))
		con_api.commit()	

		return -1
		#######

	def shop_action_calculate(self,program_id,source,batch_id,target,con_shop,cursor_shop,con_api,cursor_api):	
	
		register_action_df = pd.read_sql("""
			select p.post_date as action_date, um.meta_value as email from wp_posts as p 
			inner join wp_woocommerce_order_items as woi on woi.order_id = p.ID 
			inner join wp_postmeta as pm on pm.post_id = p.ID
			inner join wp_users as u on u.ID = pm.meta_value and pm.meta_key = '_customer_user'
			inner join wp_usermeta as um on um.user_id = u.ID and um.meta_key = 'billing_email'
			 where woi.order_item_name like '%{target}%'
		""".format(target=target),con=con_shop)
		## keep the max one
		idx = register_action_df.groupby(['email'])['action_date'].transform(max) == register_action_df['action_date']
		register_action_df = register_action_df.ix[idx,:]
		## remove duplicate
		register_action_df = register_action_df.drop_duplicates(take_last=True)	
	

		register_send_df = pd.read_sql("""
			select updated_date as send_date ,email from log_edm_response where response = '<Response [200]>' and program_id='{program_id}' and batch_id='{batch_id}'
			""".format(program_id=program_id,batch_id=batch_id),con=con_api)		


		all_ = pd.merge(register_send_df, register_action_df, 
		                            how='left',
		                            left_on=['email'], 
		                            right_on=['email'],
		                            left_index=False, right_index=False, sort=True, copy=False) 	
			

		number_action = sum(all_.send_date < all_.action_date)	
			
		cursor_api.execute("""
		    UPDATE edm_list
		    set number_action = '{number_action}'
		    WHERE program_id = '{program_id}' and batch_id = '{batch_id}' and source='{source}' and target='{target}'
		    """.format(number_action=number_action,program_id=program_id,batch_id=batch_id,source=source,target=target))
		con_api.commit()	


		return -1


	def run(self):

		con_api, cursor_api= self.connect(config.ApiMySqlConfig)
		con_event, cursor_event= self.connect(config.EventMySqlConfig)
		con_shop, cursor_shop= self.connect(config.ShopMySqlConfig)


		edm_list = pd.read_sql("""
			select * from edm_list
			""",con=con_api)		

		for i in range(edm_list.shape[0]):
			print i
			program_id = edm_list['program_id'][i]
			batch_id = edm_list['batch_id'][i]
			source = edm_list['source'][i]
			target = edm_list['target'][i]
			print source

			if source == 'shop':
				self.shop_action_calculate(program_id,source,batch_id,target,con_shop,cursor_shop,con_api,cursor_api)
			elif source == 'event':
				self.event_action_calculate(program_id,source,batch_id,target,con_event,cursor_event,con_api,cursor_api)



## huisun
# PYTHONPATH='' luigi --module edm_tasks DailyEdmHuiSun --local-scheduler
class DailyEdmHuiSun(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 4
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 700
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/huisun.html',context)
 	
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'專屬PB會員回饋活動-惠蓀林場一百周年體驗賽','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


class DailyEdmHuiSun(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 4
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 700
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/huisun.html',context)
 	
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'專屬PB會員回饋活動-惠蓀林場一百周年體驗賽','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

class DailyEdmHuiSunDiscount(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 4
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 700
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/huisun_discount.html',context)
 	
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'專屬PB+會員回饋活動-惠蓀林場一百周年體驗賽','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


class DailyShopDiscount(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 5
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' and CURDATE() < '2016-10-17' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/shop_edm.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'【好物加】會員獨享 $100 元折價優惠！限時五天！','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

# TS discount
class DailyTSDiscount(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 6
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' and CURDATE() < '2016-10-31' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/ts_edm.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'✨球星識別商品✨下殺↘$399','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

# wjs
class DailyWJS(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 7
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/ts_edm.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'2017新北市萬金石馬拉松賽事報名','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# hcc_marathon
# PYTHONPATH='' luigi --module edm_tasks DailyHccMarathon --local-scheduler
class DailyHccMarathon(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 8
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/hcc_marathon.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[好物加] 🎉新竹馬官方紀念商品限量販售🎉','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# lin32_music
# PYTHONPATH='' luigi --module edm_tasks DailyLin32Music --local-scheduler
class DailyLin32Music(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 9
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/lin32_music.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[好物加] 限定79折優惠🎉Only for林智勝音樂會球迷！','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()



# itri_smart
# PYTHONPATH='' luigi --module edm_tasks DailyItriSmart --local-scheduler
class DailyItriSmart(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 10
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 900
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/itri_smart.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[好物加] ✨首賣 3 折✨ 最貼身的運動教練 - 智慧機能緊身衣！！','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

# wjs_draw_shop
# PYTHONPATH='' luigi --module edm_tasks DailyWjsDrawShop --local-scheduler
class DailyWjsDrawShop(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 11
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>'
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/wjs_draw_shop.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'2017萬金石馬拉松限定周邊 獨享優惠8折','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()



# JZ_Fitness
# PYTHONPATH='' luigi --module edm_tasks DailyJZFitness --local-scheduler
class DailyJZFitness(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 12
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/JZ_Fitness.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[報名加] ✨現正報名中🎉筋肉媽媽祕技大公開，教你過年吃爽不發胖🎉','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# JZ_Fitness
# PYTHONPATH='' luigi --module edm_tasks DailyWJSDraw --local-scheduler
class DailyWJSDraw(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 13
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/wjsDraw.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'qq','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# PYTHONPATH='' luigi --module edm_tasks DailyNewYear2017 --local-scheduler
class DailyNewYear2017(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 14
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 45000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/newyear_2017.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'pb+ 祝大家新的一年 - 鶴立雞群！大雞大利！','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()



# PYTHONPATH='' luigi --module edm_tasks DailyWBC2017 --local-scheduler
class DailyWBC2017(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 15
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/wbc2017.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[報名加] 2017 WBC棒球經典賽購票優惠大放送！購票即享有8折優惠碼！','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

# PYTHONPATH='' luigi --module edm_tasks DailyWBC2017Coupon --local-scheduler
class DailyWBC2017Coupon(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 16
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/wbc2017_coupon.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[好物加] 2017WBC報名加購票 獨享賽事商品8折優惠碼','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# PYTHONPATH='' luigi --module edm_tasks MonthlyJanuary --local-scheduler
class MonthlyJanuary(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 17
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/MonthlyJanuary.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'pb+ 一月號「創刊號」','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()

# PYTHONPATH='' luigi --module edm_tasks Dream201701 --local-scheduler
class Dream201701(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 18
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/dream201701.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'[圓夢加]愛無界限・球具公益競標1/25 12:00準時開搶','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


# PYTHONPATH='' luigi --module edm_tasks DailySpecial --local-scheduler
class DailySpecial(luigi.Task):
	
	batch = luigi.Parameter()

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor

	def render(self,tpl_path, context):
	    path, filename = os.path.split(tpl_path)
	    return jinja2.Environment(
	        loader=jinja2.FileSystemLoader(path or './')
	    ).get_template(filename).render(context)


	def run(self):

		program = 19
		batch = self.batch

		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 50000
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)
		for row in cursor.fetchall():
			hash_ ,mail = row 

			context = {
    		'hash': hash_,
    		'program': program,
    		'batch': batch
			}

			message = self.render('edm/templates/everydayspecial.html',context)
 			
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'最低68折起！每日主打星天天超低價！','program':program,'batch':batch})	
			print r
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()