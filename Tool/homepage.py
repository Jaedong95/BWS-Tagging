import os 
import argparse
import json
import pandas as pd 
from database import BWSDB
from attrdict import AttrDict
from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/index/<int:question_no>', methods=['GET', 'POST'])
def index(question_no):
    '''
    question_no: 1 ~ 400 (총 400문항의 질문지, 각 질문지는 4개 문항으로 구성돼 있음)
    '''
    bws_df = bwsdb.bws_df   
    bws_set_df = bwsdb.bws_set_df   
    bwsdb.get_set_idx()    # 작업중인 BWS Set 번호 불러옴    
    
    # print(f'{set_idx}번 BWS Set 작업 중..')
    select_set = bws_set_df[bws_set_df.Set_no==bwsdb.set_idx]    # 선택한 BWS Set 데이터 불러옴  
    select_set.reset_index(inplace=True, drop=True)    
    question_idx = (bwsdb.set_idx-1) * 400 + (question_no-1)    # 1~3200
    
    # print(f'해당 질문지 내용: {bws_set_df.loc[question_idx]}')
    idx_list = list(map(int, select_set.iloc[question_no - 1, 3:7]))   # 4가지 질문 인덱스 불러옴 
    print(f'idx_list: {idx_list}')
    txt_list = bws_df.loc[idx_list].text_kor.values.tolist()    # 해당 페이지에서 기본적으로 보여지는 텍스트 언어 설정 (영어: text, 한글: text_kor)
    # print(question_idx)
    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(cli_argse.criteria, q_idx=question_idx)   # 주석 작업 완료된 질문지인지 확인  
    print(f'test: {checked}, {checked2}')
    print(f'test2: {checked_idx},{checked_idx2}')
    weak_idx = 9999    
    strong_idx = 9999
    if checked == 1:   # Weakest 문항이 이미 주석되어 있는 경우 
        weak_idx = idx_list.index(checked_idx)    
    if checked2 == 1:   # Strongest 문항이 이미 주석되어 있는 경우 
        strong_idx = idx_list.index(checked_idx2)
    
    '''
    Tagging 작업 
    '''
    if request.method == 'POST':
        try:
            request.form['translate']    # 텍스트 번역 작업 (한글 -> 영어)  
            txt_list = bws_df.loc[idx_list].text.values.tolist()    # 영어 질문지 반환  
        except:
            try:
                request.form['update']   # 주석 작업 잘못한 경우 초기화 (Strongest, Weakest 모두 체크되어 있는 상태에서만 동작) 
                bwsdb.update_db(cli_argse.criteria, strong_idx=idx_list[int(strong_idx)], weak_idx=idx_list[int(weak_idx)])
                bwsdb.update_log(cli_argse.criteria, q_idx=question_idx)   
                checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(cli_argse.criteria, q_idx=question_idx)
                print(checked_idx, checked_idx2)
                weak_idx = checked_idx 
                strong_idx = checked_idx2
            except:
                try:
                    weak_idx2 = request.form['radioOpt1']    # Weakest 문항 정보 저장 
                    bwsdb.save_db(cli_argse.criteria, ws_idx=idx_list[int(weak_idx2)], label_type=0)   
                    bwsdb.save_log(cli_argse.criteria, ws_idx=idx_list[int(weak_idx2)], q_idx= question_idx, label_type=0) 
                    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(cli_argse.criteria, q_idx=question_idx)
                    if checked == 1: 
                        weak_idx = idx_list.index(checked_idx)
                    if checked2 == 1:
                        strong_idx = idx_list.index(checked_idx2)
                except:
                    strong_idx2 = request.form['radioOpt2']    # Strongest 문항 정보 저장 
                    bwsdb.save_db(cli_argse.criteria, ws_idx=idx_list[int(strong_idx2)], label_type=1)
                    bwsdb.save_log(cli_argse.criteria, ws_idx=idx_list[int(strong_idx2)], q_idx=question_idx, label_type=1)
                    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(cli_argse.criteria, q_idx=question_idx)
                    if checked == 1: 
                        weak_idx = idx_list.index(checked_idx)
                    if checked2 == 1:
                        strong_idx = idx_list.index(checked_idx2)               
    if question_no == 1:    # 1번 (첫번째) 질문지 작업 
        # print(f'{bwsdb.set_idx}번 BWS Set 작업 중..')
        return render_template('index_s.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)
    elif question_no == 400:    # 400번 (마지막) 질문지 작업
        return render_template('index_e.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)
    else:    # 2 ~ 399 질문지 작업 
        return render_template('index.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)

@app.route('/', methods=['GET', 'POST'])
def main_page():
    global set_idx
    set_idx=9999
    not_tagged = []
    global bwsdb
    global annot_check
    annot_check = 9999
    
    with open(os.path.join(cli_argse.config_path, cli_argse.db_config)) as f:
        args = AttrDict(json.load(f))
    bwsdb = BWSDB(args)
    bwsdb.connect()    # MySQL 연동 
    bwsdb.get_bws(cli_argse.criteria)   
    bwsdb.get_bws_set(cli_argse.criteria)
    bwsdb.get_set_idx()
    bwsdb.table_to_csv(bwsdb.bws, 0)    # bws table -> bws dataframe 
    bwsdb.table_to_csv(bwsdb.bws_set, 1)    # bws set table -> bws set dataframe 
    
    if request.method == 'POST':
        try:
            idx = request.form['form_test']   # 1~8번 중 어떤 BWS Set 선택했는지 정보 조회 
            set_idx = int(idx)    # BWS Set 설정 
            bwsdb.save_set_idx(set_idx)    
        except:
            try:
                request.form['check_annot']   # 주석 작업되지 않은 질문지 인덱스 정보 조회 
                annot_check = 0
                bwsdb.get_bws_set_log(cli_argse.criteria)    # 주석 작업 기록 로드 
                bwsdb.table_to_csv(bwsdb.bws_set_log, 2)   # bws set log table -> bws set log db 
                bws_set_log_df = bwsdb.bws_set_log_df
                select_set = bws_set_log_df[bws_set_log_df.Set_no==bwsdb.set_idx]   
                select_set.reset_index(inplace=True, drop=True)
                set_a = select_set[select_set.Weak_checked==0].index.tolist()   
                set_b = select_set[select_set.Strong_checked==0].index.tolist()
                not_tagged = list(set(set_a + set_b)) 
                print(not_tagged)    
            except:
                print('result do not work')
    return render_template('main.html', annot_check=annot_check, set_idx=bwsdb.set_idx, not_tagged=not_tagged)

def main():
    # app.run(debug=True, port=9509)
    app.run(host="192.168.123.101", debug=True, port=9509)
    
if __name__ == '__main__':
    global cli_argse 
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config_path", type=str, default='config')
    cli_parser.add_argument("--db_config", type=str, default='db_config.json')
    cli_parser.add_argument("--criteria", type=int)
    cli_argse = cli_parser.parse_args()
    main()