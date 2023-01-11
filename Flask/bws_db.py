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

    def save_log(self, ws_idx=None, q_idx=None, label_type=None):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        ''' 
        if label_type == 0:
            sql = f'UPDATE bws_log_test SET Weak_checked = 1, Weakest = {ws_idx} WHERE idx={q_idx};'
        elif label_type == 1:
            sql = f'UPDATE bws_log_test SET Strong_checked = 1, Strongest = {ws_idx} WHERE idx={q_idx};'
        self.execute_sql(sql)
        self.conn.commit()

    def is_checked(self, q_idx):
        sql = f'SELECT Weak_checked, Strong_checked, Weakest, Strongest FROM bws_log_test WHERE idx = {q_idx};'
        result = self.execute_sql(sql)
        checked, checked2, checked_idx, checked_idx2 = result[0][0], result[0][1], result[0][2], result[0][3]
        return checked, checked2, checked_idx, checked_idx2
    
    def update_log(self, q_idx=None):
        sql = f'UPDATE bws_log_test SET Weak_checked = 0, Strong_checked = 0, Weakest = 9999, Strongest = 9999 WHERE idx={q_idx};'
        self.execute_sql(sql)
        self.conn.commit()

    def update_db(self, strong_idx=None, weak_idx=None):
        '''
        해당 질문지에 체크된 값을 해제함 
        '''
        sql = f'UPDATE bws_test SET weakest_cnt = weakest_cnt - 1 WHERE idx={weak_idx}'
        sql2 = f'UPDATE bws_test SET strongest_cnt = strongest_cnt - 1 WHERE idx={strong_idx}'
        self.execute_sql(sql)
        self.execute_sql(sql2)
        self.conn.commit()
    
    def save_db(self, ws_idx=None, label_type=None):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        '''
        if label_type == 0:
            sql = f'UPDATE bws_test SET weakest_cnt = weakest_cnt + 1 WHERE idx={ws_idx};'
        elif label_type == 1:
            sql = f'UPDATE bws_test SET strongest_cnt = strongest_cnt + 1 WHERE idx={ws_idx};'
        self.execute_sql(sql)
        self.conn.commit()