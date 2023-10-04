import os 
import csv 
import pymysql
import pandas as pd 

'''
MySQLDB Class: Best-Worst Tagging 작업을 위한 사전 준비에 사용하는 데이터베이스 
BWSDB Class: Best-Worst Tagging 작업 시 사용하는 데이터베이스 
'''
class MySQLDB():
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

    def create_idx_tb(self):
        sql = "CREATE TABLE bws_idx_table(idx int NOT NULL, \
            set_idx int, \
            PRIMARY KEY(idx) \
        )"
        sql2 = "INSERT INTO bws_idx_table VALUES (0, 9999)"
        self.execute_sql(sql)
        self.execute_sql(sql2)
        self.conn.commit()

    def create_crieria_tb(self, criteria):
        sql = f"CREATE TABLE bws_a{criteria}(idx int NOT NULL, \
            eng_text varchar(1000) NOT NULL, \
            kor_text varchar(1000) NOT NULL, \
            cnt int NOT NULL, \
            weakest_cnt int NOT NULL,\
            strongest_cnt int NOT NULL, \
            PRIMARY KEY(idx) \
        )"

        sql2 = f"ALTER TABLE bws_a{criteria} convert to charset utf8;"   # 한국어 입력 허용
        self.execute_sql(sql)
        self.execute_sql(sql2)
        self.conn.commit()
    
    def create_set_tb(self, criteria):
        sql = f"CREATE TABLE bws_a{criteria}_set(idx int NOT NULL, \
            Question_no varchar(100) NOT NULL, \
            Set_no int NOT NULL, \
            idx1 int NOT NULL, \
            idx2 int NOT NULL,\
            idx3 int NOT NULL, \
            idx4 int NOT NULL, \
            PRIMARY KEY(idx) \
        )"
        self.execute_sql(sql)
        self.conn.commit()
        
    def create_log_tb(self, criteria):
        sql = f"CREATE TABLE bws_a{criteria}_set_log(idx int NOT NULL, \
            Question_no varchar(100) NOT NULL, \
            Set_no int NOT NULL, \
            Weak_checked int NOT NULL, \
            Strong_checked int NOT NULL,\
            Weakest int NOT NULL, \
            Strongest int NOT NULL, \
            PRIMARY KEY(idx)\
        )"
        self.execute_sql(sql)
        self.conn.commit()

    def drop_tb(self, tb_name):
        sql = f"DROP TABLE {tb_name}"
        self.execute_sql(sql)
        self.conn.commit()

    def upload_criteria(self, criteria, file_name):
        file = open(file_name ,'r', encoding='UTF8')
        fReader = csv.reader(file)
        for idx, line in enumerate(fReader):
            if idx == 0: 
                continue 
            try:    # ' -> 제거하지 않으면 오류 발생 
                sql = f"INSERT INTO bws_a{criteria} VALUES('{{0}}', '{{1}}', '{{2}}', '{{3}}', '{{4}}', '{{5}}')"\
                .format(line[0], line[1].replace("'", ''), line[2].replace("'",''), line[3], line[4], line[5])
                self.curs.execute(sql)
            except:
                continue    
        self.conn.commit()

    def upload_set(self, criteria, file_name):
        file = open(file_name,'r', encoding='UTF8')
        fReader = csv.reader(file)

        for idx, line in enumerate(fReader):
            if idx == 0: 
                continue 
            
            try:  
                sql = f"INSERT INTO bws_a{criteria}_set VALUES('{{0}}', '{{1}}', '{{2}}', '{{3}}', '{{4}}', '{{5}}', '{{6}}')"\
                .format(line[0], line[1], line[2], line[3], line[4], line[5], line[6])
                self.curs.execute(sql)
            except:
                continue     
        self.conn.commit()

    def upload_log(self, criteria, file_name):
        file = open(file_name,'r', encoding='UTF8')
        fReader = csv.reader(file)

        for idx, line in enumerate(fReader):
            if idx == 0: 
                continue 
            
            try:  
                sql = f"INSERT INTO bws_a{criteria}_set_log VALUES('{{0}}', '{{1}}', '{{2}}', '{{3}}', '{{4}}', '{{5}}', '{{6}}')"\
                .format(line[0], line[1], line[2], line[3], line[4], line[5], line[6])
                self.curs.execute(sql)
            except:
                continue   
        self.conn.commit()

    def cancel_connect(self):
        self.curs.fetchall()
        self.curs.close()
        self.conn.commit()
        self.conn.close()

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

    def get_bws_set(self, criteria):
        sql = f'SELECT * FROM bws_a{criteria}_set;'
        self.bws_set = self.execute_sql(sql)

    def get_bws(self, criteria):
        sql = f'SELECT * FROM bws_a{criteria};'
        self.bws = self.execute_sql(sql)

    def get_bws_set_log(self, criteria):
        sql = f'SELECT * FROM bws_a{criteria}_set_log;'
        self.bws_set_log = self.execute_sql(sql)

    def get_set_idx(self):
        sql = 'SELECT set_idx FROM bws_idx_table;'
        self.set_idx = self.execute_sql(sql)[0][0]

    def save_set_idx(self, set_idx):   # 선택한 BWS Tagging Set 번호 저장 
        sql = f'UPDATE bws_idx_table SET set_idx={set_idx} WHERE idx=0'
        self.execute_sql(sql)
        self.conn.commit()

    def table_to_csv(self, tb, data_type):
        '''
        MySQL Table 데이터를 csv 파일로 변환
        data_type = 0  -> bws data 
        data_type = 1  -> bws set 
        data_type = 2  -> bws set log 
        '''
        if data_type == 0:
            columns=['idx', 'text', 'text_kor', 'cnt', 'weakest_cnt', 'strongest_cnt']
            self.bws_df = pd.DataFrame(tb, columns=columns)
        elif data_type == 1:
            columns=['idx', 'Question_no', 'Set_no', 'idx1', 'idx2', 'idx3', 'idx4']
            self.bws_set_df = pd.DataFrame(tb, columns=columns)
        elif data_type == 2: 
            columns=['idx', 'Question_no', 'Set_no', 'Weak_checked', 'Strong_checked', 'Weakest', 'Strongest']
            self.bws_set_log_df = pd.DataFrame(tb, columns=columns)

    def save_log(self, criteria, ws_idx=None, q_idx=None, label_type=None):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        ''' 
        if label_type == 0:
            sql = f'UPDATE bws_a{criteria}_set_log SET Weak_checked = 1, Weakest = {ws_idx} WHERE idx={q_idx};'
        elif label_type == 1:
            sql = f'UPDATE bws_a{criteria}_set_log SET Strong_checked = 1, Strongest = {ws_idx} WHERE idx={q_idx};'
        self.execute_sql(sql)
        self.conn.commit()

    def is_checked(self, criteria, q_idx):
        '''
        주석 작업되었는지 확인 
        '''
        sql = f'SELECT Weak_checked, Strong_checked, Weakest, Strongest FROM bws_a{criteria}_set_log WHERE idx = {q_idx};'
        result = self.execute_sql(sql)
        checked, checked2, checked_idx, checked_idx2 = result[0][0], result[0][1], result[0][2], result[0][3]
        return checked, checked2, checked_idx, checked_idx2
    
    def update_log(self, criteria, q_idx=None):
        '''
        주석을 잘못한 경우, 기록을 초기화하는 함수 (default: 9999)
        '''
        sql = f'UPDATE bws_a{criteria}_set_log SET Weak_checked = 0, Strong_checked = 0, Weakest = 9999, Strongest = 9999 WHERE idx={q_idx};'
        self.execute_sql(sql)
        self.conn.commit()

    def update_db(self, criteria, strong_idx=None, weak_idx=None):
        '''
        주석을 잘못한 경우, 해당 질문지에 체크된 값을 해제함 
        '''
        sql = f'UPDATE bws_a{criteria} SET weakest_cnt = weakest_cnt - 1 WHERE idx={weak_idx}'
        sql2 = f'UPDATE bws_a{criteria} SET strongest_cnt = strongest_cnt - 1 WHERE idx={strong_idx}'
        self.execute_sql(sql)
        self.execute_sql(sql2)
        self.conn.commit()
    
    def save_db(self, criteria, ws_idx=None, label_type=None):
        '''
        label_type = 0  -> weakest 
        label_type = 1  -> strongest 
        '''
        if label_type == 0:
            sql = f'UPDATE bws_a{criteria} SET weakest_cnt = weakest_cnt + 1 WHERE idx={ws_idx};'
        elif label_type == 1:
            sql = f'UPDATE bws_a{criteria} SET strongest_cnt = strongest_cnt + 1 WHERE idx={ws_idx};'
        self.execute_sql(sql)
        self.conn.commit()

