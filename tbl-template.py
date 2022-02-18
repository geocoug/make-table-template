#! /usr/bin/python
# -*- coding: utf-8 -*-
# tbl-template.py

"""
PURPOSE
    Create an ODS file containing column names matching an input
    list of database tables.

TODO
    1) Create file subdirectories as needed to write to ODS.
    2) Allow output in XLSX format.
    3) Automatically exclude all SERIAL and BIGSERIAL columns
    4) Allow optional reading from a configuration file.

AUTHOR
    Caleb Grant (CG)

HISTORY
    Date          Remarks
    ----------	--------------------------------------------------
    2022-02-17    Created.  CG.
    2022-02-18    Add table pattern matching. CG.
=================================================================="""

import psycopg2
import odf.opendocument
import odf.table
import argparse
import getpass
import io
import re

__version__ = "0.1.1"
__vdate = "2022-02-18"


class Database():
    '''Base class to connect to a database and execute procedures.'''
    def __init__(self, host, db, user, password=None):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.conn = None
        self.password = self.get_password()

    def get_password(self):
        return getpass.getpass("Enter your password for %s" % self.__repr__())

    def open_db(self):
        if self.conn is not None:
            self.conn.close()
        if self.password is not None:
            try:
                self.conn = psycopg2.connect(f"""
                    host='{self.host}'
                    dbname='{self.db}'
                    user='{self.user}'
                    password='{self.password}'""")
            except psycopg2.OperationalError as err:
                raise err

    def cursor(self):
        if self.conn is None:
            self.open_db()
        return self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql):
        cur = self.cursor()
        try:
            cur.execute(sql)
        except Exception as err:
            raise err
        return cur

    def __repr__(self):
        return u"""Database(host=%s, database=%s, user=%s)""" % (self.host, self.db, self.user)


class DatabaseQuery(Database):
    '''Database subclass used to perform customized methods.'''
    def __init__(self, host, db, user, schema, tbls, xcols=None):
        super().__init__(host, db, user)
        self.schema = schema
        self.tbls = tbls
        if xcols is None:
            self.xcols = xcols
        else:
            self.xcols = xcols.strip()
        self.tbl_list = self.table_list()

    def table_list(self):
        '''Return list of schema tables.\n
        TODO - Add pattern matching (ie. e_*,x_* )
        '''
        tbl_list = []
        if self.tbls is None:
            tables = self.execute(f"""SELECT table_name
                                  FROM information_schema.tables
                                  WHERE table_schema = '{self.schema}'
                                  ORDER BY table_name;""")
            tbl_list = [table[0] for table in tables]
        else:
            self.tbls = self.tbls.strip()
            try:
                tables = [t for t in self.tbls.split(',')]
                for tbl in tables:
                    if re.search('\\*', tbl):
                        match_str = tbl.split('*')
                        match_str = '%'.join(match_str)
                        tbl_list.extend(self.match_tbl(match_str))
                    if self.verify_tbl(tbl):
                        tbl_list.append(tbl)
                    else:
                        pass
            except Exception as err:
                raise err
        return sorted(list(set(tbl_list)))

    def match_tbl(self, tbl):
        cur = self.execute(f"""SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = '{self.schema}'
                            AND table_name ilike '{tbl}';""")
        return [t[0] for t in cur]

    def verify_tbl(self, tbl):
        c = self.execute(f"""SELECT *
                        FROM information_schema.tables
                        WHERE table_schema = '{self.schema}'
                        AND table_name = '{tbl}';""")
        if c.fetchone() is None:
            return False
        else:
            return True

    def column_list(self, tbl):
        cols = self.execute(f"""SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = '{self.schema}'
                            AND table_name = '{tbl}';""")
        if self.xcols is None:
            return [col[0] for col in cols]
        else:
            return [col[0] for col in cols
                    if col[0] not in self.xcols.split(',')]


class OdsFile():
    '''Create, write, and save to an OpenDatasheet file.'''
    def __init__(self):
        self.filename = None
        self.wkbook = None

    def open(self, filename):
        self.filename = filename
        self.wkbook = odf.opendocument.OpenDocumentSpreadsheet()

    def new_sheet(self, sheetname):
        return odf.table.Table(name=sheetname)

    def add_row_to_sheet(self, datarow, odf_table):
        tr = odf.table.TableRow()
        odf_table.addElement(tr)
        for item in datarow:
            tr.addElement(odf.table.TableCell(valuetype="string",
                                              stringvalue=item))

    def add_sheet(self, odf_table):
        self.wkbook.spreadsheet.addElement(odf_table)

    def save_close(self):
        ofile = io.open(self.filename, "wb")
        self.wkbook.write(ofile)
        ofile.close()


def clparser():
    '''Create a parser to handle input arguments and displaying
    a script specific help message.'''
    desc_msg = """Create an ODF file of empty data tables with
        matching columns to the specified database tables.
        Version %s, %s""" % (__version__, __vdate)
    parser = argparse.ArgumentParser(description=desc_msg)
    parser.add_argument('output_file',
                        help="""Name of the ODS output file containing
                        data table templates.""")
    parser.add_argument('-v', '--host', type=str, default='env3', dest='host',
                        help="Server hostname.")
    parser.add_argument('-d', '--database', type=str, dest='database',
                        help="Database name.")
    parser.add_argument('-s', '--schema', type=str, dest='schema',
                        help='Database schema.')
    parser.add_argument('-u', '--username', type=str, dest='username',
                        help="Database username.")
    parser.add_argument('-t', '--tables', type=str, dest='table_list',
                        help="""Comma seperated list of tables for template
                        creation. If no tables are specified, all schema tables
                        will be used. Pattern matching may be used to find
                        table similarities (ie. x_* or *lab*).""")
    parser.add_argument('-x', '--columns', type=str, dest='x_columns',
                        help="""Comma seperated list of columns that should not be
                        included in the output table templates.""")
    return parser


if __name__ == "__main__":
    parser = clparser()
    args = parser.parse_args()

    db_inst = DatabaseQuery(
        args.host,
        args.database,
        args.username,
        args.schema,
        args.table_list,
        args.x_columns
    )

    wkbook = OdsFile()
    wkbook.open(args.output_file)

    for tbl in db_inst.tbl_list:
        tbl_sheet = wkbook.new_sheet(tbl)
        wkbook.add_row_to_sheet(tuple(db_inst.column_list(tbl)), tbl_sheet)
        wkbook.add_sheet(tbl_sheet)

    wkbook.save_close()
    db_inst.close()
