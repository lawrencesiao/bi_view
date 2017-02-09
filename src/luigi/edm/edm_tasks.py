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
	
	batch = luigi.Parameter()#
	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		cursor.execute('SET NAMES UTF8')
		cursor.execute(self.query)
		return connection, cursor#
	def run(self):
		program = 4
		batch = self.batch
#    	batch = 1
		self.query = """
			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 700
		""".format(program=program,batch=batch)
		connection, cursor= self.connect(config.ApiMySqlConfig)

		for row in cursor.fetchall():
			hash_ ,mail = row 
			message = """
			<html>
			<body>
			<br>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/61/info&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">
			<img src="https://api.pbplus.me/track/redirect?url=https://s3-ap-northeast-1.amazonaws.com/pbplus-event/event/61/banner.jpg&hash={hash}&type=load_img&program={program}&batch={batch}&source=edm
			" alt="HuiSun" width="500">
			</a>
			<br>
			<br>
			<p>時光如旅，總有起點才能迎向那未知的每一步。</p>
			<p>而叢林間的小樹，隨著小溪的呼吸聲而逐漸壯大。</p>
			<p>每一步的印記，都是璀璨、繽紛的回憶與標記。</p>
			<p>然而，一生只有一次，那就該，什麼都試一試。 </p>
			<br>
			惠蓀林場一百歲了！我們規劃了精采的活動、舒適的住處、美味的食物，這一切都為了與您在這美麗的山林相遇。<br>
			屆時將有101隊的好朋友們於惠蓀林場奔跑跳躍、盡情歡呼，我們誠摯地邀請您與我們用心靈與五感，一同感受“新森活”。<br>
			※本活動包含 定向越野積分賽 與 一騎跑計時賽 (每隊兩人)※<br>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/61/info&hash={hash}&type=click_link_up&program={program}&batch={batch}&source=edm">立即報名</a>
			<br>
			定向越野積分賽<br>
			● 大會將依報名組別 (休閒組 / 挑戰組) 提供專屬地圖<br>
			● 限制時間內，依各隊打卡點分數做為成績依據<br>
			● 各隊於限制時間內回到主會場進行一騎跑轉換<br>
			<br>
			一騎跑計時賽<br>
			● 一人跑步 + 一人騎單車方式完成比賽，兩人可交替運動方式<br>
			● 上坡段：一人騎 + 一人跑 ─ 折返點：放車 ─ 下坡段：兩人跑<br>
			● 大會將訂定完賽時間轉換級距，依照級距進行時間與分數轉換<br>
			<br>
			大會服務內容<br>
			● 會場住宿 (大會統一安排住宿)<br>
			● 賽事餐飲、補給<br>
			● 賽事物資 (地圖、紀念衣、號碼布) 以報名加實際公告為主<br>
			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/61/info&hash={hash}&type=click_link_down&program={program}&batch={batch}&source=edm">立即報名</a>
			<br>
			<br>
			</body>
			</html>
				""".format(hash=hash_,program=program,batch=batch)		
			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'專屬PB會員回饋活動-惠蓀林場一百周年體驗賽','program':program,'batch':batch})	
			cursor.execute("""
		                UPDATE log_edm_response 
		                SET response = '{response}', updated_date='{updated_date}'
		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#
			cursor.connection.commit()
        
		connection.close()


### huisun
## PYTHONPATH='' luigi --module edm_tasks DailyEdmHuiSun --local-scheduler
#class DailyEdmHuiSun(luigi.Task):
#	
#	batch = luigi.Parameter()#

#	def connect(self, datasource):
#		connection = datasource().connection()
#		cursor = connection.cursor()
#		cursor.execute('SET NAMES UTF8')
#		cursor.execute(self.query)
#		return connection, cursor#

#	def run(self):
#		program = 4
#		batch = self.batch
##    	batch = 1
#		self.query = """
#			SELECT hash,email from log_edm_response where program_id ='{program}' and batch_id = '{batch}' and response != '<Response [200]>' LIMIT 200
#		""".format(program=program,batch=batch)#

#		connection, cursor= self.connect(config.ApiMySqlConfig)#

#		for row in cursor.fetchall():
#			hash_ ,mail = row 
#			message = """
#			<html>
#			<body>
#			<center>
#			<br>
#			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/61/info&hash={hash}&type=click_photo&program={program}&batch={batch}&source=edm">
#			<img src="https://api.pbplus.me/track/redirect?url=https://s3-ap-northeast-1.amazonaws.com/pbplus-event/event_img/edm/HuiSun.jpg&hash={hash}&type=load_img&program={program}&batch={batch}&source=edm
#			" alt="HuiSun" width="750">
#			</a>
#			<br>
#			<strong>
#			惠蓀林場一百歲了！我們規劃了精采的活動、舒適的住處、美味的食物，這一切都為了與您在這美麗的山林相遇。<br>
#			</strong>
#			<br>
#			<a href="https://api.pbplus.me/track/redirect?url=https://event.pbplus.me/event/61/info&hash={hash}&type=click_link_down&program={program}&batch={batch}&source=edm">立即報名</a>
#			<br>
#			<br>
#			</center>
#			</body>
#			</html>
#				""".format(hash=hash_,program=program,batch=batch)		#

#			r = requests.post('https://api.pbplus.me//edm/send', data = {'mail_list':mail,'message':message,'subject':'專屬PB會員回饋活動-惠蓀林場一百周年體驗賽','program':program,'batch':batch})	
#			cursor.execute("""
#		                UPDATE log_edm_response 
#		                SET response = '{response}', updated_date='{updated_date}'
#		                WHERE email = '{mail}' and program_id = '{program}' and batch_id = '{batch}'
#			""".format(mail=mail,response=str(r),program=program,batch=batch,updated_date=datetime.datetime.now()))#

#			cursor.connection.commit()
#        
#		connection.close()