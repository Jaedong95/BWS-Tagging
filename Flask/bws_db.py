import pymysql
import pandas as pd 

class BWSDB():
    def __init__(self, args):
        self.args = args 

    def connect(self):
        self.conn = pymysql.connect(host=self.args.host, user=self.args.user, \
            password=self.args.password, db=self.args.db, charset='utf8')
        self.curs = self.conn.cursor()

    def execute_sql(self, sql):
        self.curs.execute(sql)
        return self.curs.fetchall() 

    def update_sql(self, sql):
        self.curs.execute(sql)
        self.curs.fetchall() 

    def get_bws_set(self):
        sql = 'SELECT * FROM bws_set;'
        self.bws_set = self.execute_sql(sql)

    def get_bws(self):
        sql = 'SELECT * FROM bws_table;'
        self.bws = self.execute_sql(sql)

    def table_to_csv(self, tb, data_type):
        '''
        data_type = 0  -> bws data 
        data_type = 1  -> bws set 
        '''
        if data_type == 0:
            columns=['idx', 'text', 'text_kor', 'cnt', 'weakest_cnt', 'strongest_cnt']
            self.bws_df = pd.DataFrame(tb, columns=columns)
        elif data_type == 1:
            columns=['idx', 'Question_no', 'Set_no', 'idx1', 'idx2', 'idx3', 'idx4']
            self.bws_set_df = pd.DataFrame(tb, columns=columns)

    def save_log(self):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        ''' 

    def save_db(self, label_type, ws_idx=None, idx_list=None):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        '''
        if idx_list != None:
            for idx in idx_list:
                sql = f'UPDATE bws_test SET cnt = cnt + 1 WHERE idx={idx};'

        if label_type == 0:
            sql = f'UPDATE bws_test SET weakest_cnt = weakest_cnt + 1 WHERE idx={idx};'
        elif label_type == 1:
            sql = f'UPDATE bws_test SET strongest_cnt = strongest_cnt + 1 WHERE idx={idx};'
        self.execute_sql(sql)
        self.conn.commit()
        sql = f'SELECT * FROM bws_test WHERE idx={idx}'
        print(self.execute_sql(sql))
