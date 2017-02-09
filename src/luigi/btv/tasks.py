import csv
import datetime
import luigi
import os
from plugins import mysqldb
from datasource import config
from ptt_crawler import crawl
from utils.utils import convert_timezone


# PYTHONPATH='' luigi --module btv.tasks PttCrawler --local-scheduler --date=2016-04-21
class PttCrawler(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())
    # TODO: Get start and end automatically
    start = luigi.IntParameter(default=3416)
    end = luigi.IntParameter(default=3417)

    def run(self):
        posts, replies = crawl(self.start, self.end)

        with self.output()[0].open('w') as out_file:
            writer = csv.writer(out_file)
            writer.writerows(posts)

        with self.output()[1].open('w') as out_file:
            writer = csv.writer(out_file)
            writer.writerows(replies)

    def output(self):
        return (
            luigi.LocalTarget(self.date.strftime(
                'data/btv/%Y_%m_%d/ptt/posts.csv')),
            luigi.LocalTarget(self.date.strftime(
                'data/btv/%Y_%m_%d/ptt/replies.csv'))
        )


# PYTHONPATH='' luigi --module btv.tasks PttRepliesData --local-scheduler --date=2016-04-21
class PttRepliesData(luigi.Task):

    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return PttCrawler(self.date)

    def output(self):
        return luigi.LocalTarget(self.date.strftime(
            'data/btv/%Y_%m_%d/ptt/replies.csv'))

# PYTHONPATH='' luigi --module btv.tasks ProcessPttReplies --local-scheduler --date=2016-04-21
class ProcessPttReplies(config.BiMySqlTarget, mysqldb.CopyToTable):

    date = luigi.DateParameter(default=datetime.date.today())
    year = luigi.IntParameter(default=2016)
    table = 'game_replies'
    columns = ['game_date', 'post_id', 'notation', 'topic', 'author', 'ts', 'content']

    def requires(self):
        return PttRepliesData(self.date)

    def rows(self):
        with self.input().open('r') as in_file:
            reader = csv.reader(in_file, delimiter=',', quotechar='"')
            for row in reader:
                row.insert(0, self.date.strftime('%Y-%m-%d'))
                # row[-2] = \
                #     '{}-{}'.format(self.year, row[-2].replace('/', '-'))
                row[-2] = convert_timezone(
                    '{}/{}:00'.format(self.year, row[-2])
                ).strftime('%Y-%m-%d %H:%M:%S')
                yield [cell.decode('utf-8') for cell in row]

# class ProcessLiveStreamData(luigi.Task):

#     def requires(self):
#         pass

#     def output(self):
#         return luigi.LocalTarget(self.date.strftime(
#             'data/btv/daily_btv_%Y_%m_%d.csv'))

#     def run(self):
#         pass


# $PYTHONPATH='' luigi --module btv.tasks ProcessBTVLogData --local-scheduler
# class ProcessBtvLogData(luigi.Task):

#     date = luigi.DateParameter(default=datetime.date.today())

#     def requires(self):
#         logs_path = self.date.strftime('data/btv/%Y_%m_%d/logs')
#         for filename in os.listdir(logs_path):
#             if filename.endswith('.csv'):
#                 print filename

#     def output(self):
#         return luigi.LocalTarget(self.date.strftime(
#             'data/btv/daily_btv_%Y_%m_%d.csv'))

#     def run(self):
#         print self.input()

# class ProcessPttPostData(luigi.Task):

#     date = luigi.DateParameter()

#     def requires(self):
#         pass

#     def output(self):
#         return luigi.LocalTarget(self.date.strftime(
#             'data/btv/daily_btv_%Y_%m_%d.csv'))

#     def run(self):
#         pass
