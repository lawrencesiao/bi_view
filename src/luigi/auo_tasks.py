# -*- coding: utf-8 -*-
import datetime
import luigi
import requests	

# PYTHONPATH='' luigi --module auo_tasks DailyRunKeeperGet --local-scheduler
class DailyRunKeeperGet(luigi.Task):

	date = luigi.DateParameter(default=datetime.date.today())

	def run(self):

		r1 = requests.get('http://auogames.pbplus.me/runkeeper/pb/batch?p=54883155')
		r2 = requests.get('http://auogames.pbplus.me/runkeeper/pb/batch_milestone?p=54883155')

#class DailyRunKeeperSummarize(luigi.Task):

#	date = luigi.DateParameter(default=datetime.date.today())

#	def run(self):
#		r = requests.get('http://auogames.pbplus.me/runkeeper/pb/batch_milestone?p=54883155')
