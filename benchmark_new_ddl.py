#!/usr/bin/env python
import csv
import json
import logging
import time

from psycopg2.extras import LoggingConnection, MinTimeLoggingCursor

from utils.db import get_pg_conn


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class BenchLoggingConnection(LoggingConnection):
    """A connection that logs queries based on execution time.

    Note that this connection uses the specialized cursor
    `MinTimeLoggingCursor`.
    """
    last_exec_time = None

    def initialize(self, logobj):
        LoggingConnection.initialize(self, logobj)

    def filter(self, msg, curs):
        t = (time.time() - curs.timestamp) * 1000  # Execution time (ms)
        self.last_exec_time = t
        return '{msg}\n(execution time: {t} ms)'.format(msg=msg, t=t)

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MinTimeLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


class Config(object):
    """
    Helper class for benchmark configuration settings
    """

    __slots__ = ('_config', 'groups', 'set_up', 'tear_down', 'queries')

    def __init__(self, conf_file):
        self._config = json.load(open(conf_file, 'r'))
        self.groups = self._config.get('groups', [])
        self.set_up = self._config.get('setUp', [])
        self.tear_down = self._config.get('tearDown', [])
        self.queries = self._config.get('queries', [])

    def result_header(self):
        header = ['Group Name', 'Description']
        runs = [q['name'] for q in self.queries]
        return header + runs

    def format_query_for_group(self, sql, group_name):
        """
        Get queries with table names replaced by necessary modified names
        """
        group = None
        for g in self.groups:
            if g['name'] == group_name:
                group = g
        if not group or group.get('isControl'):
            return sql

        for table in group.get('tables', []):
            name = table['name']
            sql = sql.replace(name, '{}_{}'.format(name, group_name))
        return sql


class Columns(object):
    """
    Class to help manage and format column modifications.
    """

    __slots__ = ('cols', 'dist_key', 'sort_keys', 'sort_type', 'dist_style')

    def __init__(self, cols, mods):
        # TODO - IMPORTANT PRESERVER THE ORDER!!!!
        self.cols = cols

        self.dist_key = None
        self.sort_keys = []
        self.sort_type = 'COMPOUND'
        for col in self.cols:
            sort_key = int(col[-2])
            if sort_key != 0:
                if sort_key < 0:
                    self.sort_type = 'INTERLEAVED'
                self.sort_keys.append(col[0])
            if col[-3] and not self.dist_key:
                self.dist_key = col[0]
        self.dist_key = mods.get('distKey', self.dist_key)
        self.dist_style = mods.get('distStyle', 'KEY' if self.dist_key else 'EVEN')
        self.sort_keys = mods.get('sortKeys', self.sort_keys)
        self.sort_type = mods.get('sortType', self.sort_type)

    @staticmethod
    def format_col(col):
        encode = col[2]
        return '{colname} {typ} {encoding} {not_null}'.format(
            colname=col[0],
            typ=col[1],
            encoding='ENCODE {}'.format(encode) if encode != 'none' else '',
            not_null='NOT NULL' if col[-1] else ''
        )

    def formated_columns(self):
        return ',\n'.join([Columns.format_col(col) for col in self.cols])

    def format_dist_key(self):
        return 'DISTKEY ({})'.format(self.dist_key) if self.dist_key else ''

    def format_dist_style(self):
        return 'KEY' if self.dist_key else self.dist_style


def set_up(config, curr):
    # Custom setup queries
    for view in config.set_up:
        curr.execute(view)


def tear_down(config, curr):
    # Custom teardown queries
    for query in config.tear_down:
        curr.execute(query)


def run_queries(config, curr, conn, group_stats):
    # Run queries and record the results
    group_name = group_stats['name']
    for query in config.queries:
        sql = config.format_query_for_group(query['sql'], group_name)
        curr.execute('EXPLAIN {}'.format(sql))
        exec_times = []
        for i in range(query.get('numRuns', 3)):
            curr.execute(sql)
            exec_times.append(conn.last_exec_time)
        group_stats[query['name']] = exec_times


def drop_group_tables(tables, conn, curr, group_stats):
    # Drop all modified group-level tables
    group_name = group_stats['name']
    for table in tables:
        curr.execute('DROP TABLE IF EXISTS {schema}.{table}_{group}'.format(
            schema=table['schema'], table=table['name'], group=group_name)
        )
    conn.commit()


def run_benches(config, curr, conn, group_stats, tables=None):
    set_up(config, curr)
    run_queries(config, curr, conn, group_stats)
    if tables:
        drop_group_tables(tables, conn, curr, group_stats)
    tear_down(config, curr)


def write_results(results, config):
    with open('results.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(config.result_header())
        for result in results:
            runs = [result[q['name']] for q in config.queries]
            row = [result['name'], result['description']] + runs
            writer.writerow(row)


def main(conf_file, database=None):
    # First parse the config file and establish new connection/cursor objects
    config = Config(conf_file)
    conn, curr = get_pg_conn(database, BenchLoggingConnection, logger)

    # For pg_table_def we need to make sure stage is in the search path
    curr.execute("SET SEARCH_PATH TO '$user', public, stage")

    results = []
    for group in config.groups:
        group_name = group['name']
        group_stats = {
            'name': group_name,
            'description': group.get('description', '')
        }
        if group.get('isControl'):
            # Run queries
            run_benches(config, curr, conn, group_stats)
            results.append(group_stats)
            continue

        tables = group['tables']
        for table in tables:
            schema = table['schema']
            table_name = table['name']
            mods = table.get('mods')
            # Get current table settings
            curr.execute('''
                SELECT "column", type, encoding, distkey, sortkey, "notnull"
                FROM pg_table_def
                WHERE tablename = %s AND schemaname = %s
            ''', (table_name, schema))
            columns = Columns(list(curr.fetchall()), mods)

            curr.execute('''
                CREATE TABLE {schema}.{table}_{group} (
                    {columns}
                )
                DISTSTYLE {dist_style}
                {dist_key}
                {sort_type} SORTKEY ({sort_keys})
            '''.format(
                schema=schema,
                table=table_name,
                group=group_name,
                columns=columns.formated_columns(),
                dist_style=columns.format_dist_style(),
                dist_key=columns.format_dist_key(),
                sort_type=columns.sort_type,
                sort_keys=','.join(columns.sort_keys)
            ))

        # Commits new table creates
        conn.commit()

        run_benches(config, curr, conn, group_stats, tables)
        results.append(group_stats)

    write_results(results, config)
    curr.close()
    conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Benchmark different redshift DDLs')
    parser.add_argument('--db', '-d', default='devreal', help='Database to analyze.')
    parser.add_argument('--config', '-c', default='config.json', help='JSON configuration file')
    args = parser.parse_args()

    database = args.db
    main(args.config, database)
