# -*-coding:utf-8_-*-

import MySQLdb


# Function: mysql util class
class SqlUtil():
    def __init__(self, ip_addr, user_name, passwd, db_name, port=3306):
        self.ip_addr = ip_addr
        self.user_name = user_name
        self.passwd = passwd
        self.db_name = db_name
        self.port = port

    # Function: connect to mysql server
    def connect(self):
        try:
            # use_unicode=True
            self.conn = MySQLdb.connect(host=self.ip_addr, user=self.user_name, passwd=self.passwd,
                                        port=self.port, charset='utf8' )
            self.conn.select_db(self.db_name)
        except MySQLdb.Error, e:
            print "Mysql connect Error %s" % (e)

    # Function: disconnect to mysql server
    def disconnect(self):
        try:
            self.conn.close();
        except MySQLdb.Error, e:
            print "Mysql disconnect Error %s" % (e)

    # Function: get data from mysql db(select)
    # Parameter: mysql query cmd string
    def get_data_from_db(self, sql_cmd_str):
        try:
            cur = self.conn.cursor()
            cur.execute(sql_cmd_str)
            results = cur.fetchall()
            cur.close()
        except MySQLdb.Error, e:
            print "Mysql get_data_from_db Error %s" % (e)
            return []
        return results

    # Function: exec mysql cmd string(insert into,delete,create table,drop table...)
    # Parameter: mysql exec cmd string
    def exec_db_cmd(self, sql_cmd_str):
        try:
            cur = self.conn.cursor()
            cur.execute(sql_cmd_str)
            self.conn.commit()
            cur.close()
        except MySQLdb.Error, e:
            print "Mysql exec_db_cmd Error %s" % (e)


if __name__ == '__main__':
    sqlutil = SqlUtil('127.0.0.1', 'root', '123456', 'fm')
    sqlutil.connect()
    param = "'%"+u'亿'+"%'"
    query_sql = '''select company_name,money from fm where money like %s'''%param
    # print query_sql
    result = sqlutil.get_data_from_db(query_sql)
    for r in result:
        print r[0]+","+r[1]

    # print item
