import luigi
import tempfile
import datetime
import logging
import MySQLdb
from luigi.contrib import rdbms
from luigi.contrib.mysqldb import MySqlTarget
from luigi.postgres import MultiReplacer
from shutil import copyfile

logger = logging.getLogger('luigi-interface')

default_escape = MultiReplacer([('\\', '\\\\'),
                                ('\0', '\\0'),
                                ('\'', '\\\''),
                                ('\"', '\\"'),
                                ('\b', '\\b'),
                                ('\n', '\\n'),
                                ('\r', '\\r'),
                                ('\t', '\\t'),
                                ])


class CopyToTable(rdbms.CopyToTable):

    def rows(self):
        """Return/yield tuples or lists corresponding to each row to be
        inserted """
        # Notes: Handle multiple inputs
        inputs = self.input() if type(self.input()) is list else [self.input()]
        for input in inputs:
            with input.open('r') as fobj:
                for line in fobj:
                    yield line.strip('\n').split(self.column_separator)

    def map_column(self, value):
        """Applied to each column of every row returned by `rows`
        Default behaviour is to escape special characters and identify any
        self.null_values
        """
        if value in self.null_values:
            return 'NULL'
        elif isinstance(value, unicode):
            return default_escape(value).encode('utf8')
        else:
            return default_escape(str(value))

    def output(self):
        return MySqlTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
        )

    def copy_from(self, cursor, file_path, table, column_sep='\t',
                  row_sep='\n'):

        cursor.execute("""
            load data local infile '{file_path}' into table {table_name}
            fields terminated by '{column_sep}' lines terminated by '{row_sep}'
        """.format(file_path=file_path, table_name=table,
                   column_sep=column_sep, row_sep=row_sep))

    def copy(self, cursor, file):
        self.copy_from(cursor, file.name, self.table, self.column_separator)

    def run(self):
        """Inserts data generated by rows() into target table.
        If the target table doesn't exist, self.create_table will be called to
        attempt to create the table.
        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        tmp_dir = luigi.configuration.get_config().get(
            'mysql', 'local-tmp-dir', None)
        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
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
