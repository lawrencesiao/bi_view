# -*- coding: utf-8 -*-

import datetime
import luigi
import os
import subprocess
from datasource import config
from luigi.contrib.mysqldb import MySqlTarget
from plugins import mysqldb
import MySQLdb

# PYTHONPATH='' luigi --module event_tasks EventRemainTimeCheck --local-scheduler
class EventRemainTimeCheck(luigi.Task):

	def connect(self, datasource):
		connection = datasource().connection()
		cursor = connection.cursor()
		return connection, cursor

	def run(self):

		connection, cursor = self.connect(config.EventMySqlConfig) 
	
		self.query = "SET @before_1hour = (NOW() - INTERVAL 10 MINUTE);"
		cursor.execute(self.query)
		connection.commit()

		self.query= """
		UPDATE registration_dish
		INNER JOIN (
		   SELECT dish_id, SUM(team_max_select) as team
		   FROM registration_registerdish
		   WHERE register_id in (
		       SELECT id 
		       FROM registration_register
		       WHERE status  = 'orderDraft'
		       AND updated_date BETWEEN '2016-07-31 16:00:00' AND @before_1hour
		   )
		   GROUP BY dish_id
		) rrd
		ON registration_dish.id = rrd.dish_id
		AND registration_dish.register_count >= rrd.team
		SET registration_dish.register_count = registration_dish.register_count - rrd.team;		
		"""
		cursor.execute(self.query)
		connection.commit()

		self.query="""
		UPDATE registration_register
		SET status = 'orderCancelled'
		WHERE status  = 'orderDraft'
		AND updated_date BETWEEN '2016-07-31 16:00:00' AND @before_1hour
		"""
		cursor.execute(self.query)
		connection.commit()
		connection.close()

