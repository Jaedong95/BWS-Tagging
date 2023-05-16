import os 
import argparse
import json
import pandas as pd 
from bws_db import BWSDB
from attrdict import AttrDict
from flask import Flask, render_template, request

app = Flask(__name__)
@app.route('/index/<int:question_no>', methods=['GET', 'POST'])
def index(question_no):
    bws_df = bwsdb.bws_df
    bws_set_df = bwsdb.bws_set_df
    bwsdb.get_set_idx()
    # print(f'{set_idx}번 BWS Set 작업 중..')
    select_set = bws_set_df[bws_set_df.Set_no==bwsdb.set_idx]
    select_set.reset_index(inplace=True, drop=True)
    question_idx = (bwsdb.set_idx-1) * 400 + (question_no-1)    # 1~3200
    # print(f'해당 질문지 내용: {bws_set_df.loc[question_idx]}')
    idx_list = list(map(int, select_set.iloc[question_no - 1, 3:7]))
    # print(idx_list)
    txt_list = bws_df.loc[idx_list].text_kor.values.tolist()
    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
    # print(f'test: {checked}, {checked2}')
    weak_idx = 9999
    strong_idx = 9999
    if checked == 1: 
        weak_idx = idx_list.index(checked_idx)
    if checked2 == 1:
        strong_idx = idx_list.index(checked_idx2)
    if question_no == 1:
        print(f'{bwsdb.set_idx}번 BWS Set 작업 중..')
        if request.method == 'POST':
            try:
                request.form['translate']
                txt_list = bws_df.loc[idx_list].text.values.tolist()
            except:
                try:
                    request.form['update']
                    bwsdb.update_db(strong_idx=idx_list[int(strong_idx)], weak_idx=idx_list[int(weak_idx)])
                    bwsdb.update_log(q_idx=question_idx)
                    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                    weak_idx = checked_idx 
                    strong_idx = checked_idx2
                except:
                    try:
                        weak_idx2 = request.form['radioOpt1']
                        bwsdb.save_db(ws_idx=idx_list[int(weak_idx2)], label_type=0)   # 질문 번호 (1~1600)
                        bwsdb.save_log(ws_idx=idx_list[int(weak_idx2)], q_idx= question_idx, label_type=0) 
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)
                    except:
                        strong_idx2 = request.form['radioOpt2']
                        bwsdb.save_db(ws_idx=idx_list[int(strong_idx2)], label_type=1)
                        bwsdb.save_log(ws_idx=idx_list[int(strong_idx2)], q_idx=question_idx, label_type=1)
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)
        return render_template('index_s.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)
    elif question_no == 400:
        if request.method == 'POST':
            try:
                request.form['translate']
                txt_list = bws_df.loc[idx_list].text.values.tolist()
            except:
                try:
                    request.form['update']
                    bwsdb.update_db(strong_idx=idx_list[int(strong_idx)], weak_idx=idx_list[int(weak_idx)])
                    bwsdb.update_log(q_idx=question_idx)
                    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                    weak_idx = checked_idx 
                    strong_idx = checked_idx2
                except:
                    try:
                        weak_idx2 = request.form['radioOpt1']
                        bwsdb.save_db(ws_idx=idx_list[int(weak_idx2)], label_type=0)
                        bwsdb.save_log(ws_idx=idx_list[int(weak_idx2)], q_idx= question_idx, label_type=0)
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)
                    except:
                        strong_idx2 = request.form['radioOpt2']
                        bwsdb.save_db(ws_idx=idx_list[int(strong_idx2)], label_type=1)
                        bwsdb.save_log(ws_idx=idx_list[int(strong_idx2)], q_idx=question_idx, label_type=1)
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)               
        return render_template('index_e.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)
    else: 
        if request.method == 'POST':
            try:
                request.form['translate']
                txt_list = bws_df.loc[idx_list].text.values.tolist()
            except:
                try:
                    request.form['update']
                    bwsdb.update_db(strong_idx=idx_list[int(strong_idx)], weak_idx=idx_list[int(weak_idx)])
                    bwsdb.update_log(q_idx=question_idx)
                    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                    weak_idx = checked_idx 
                    strong_idx = checked_idx2
                except:
                    try:
                        weak_idx2 = request.form['radioOpt1']
                        bwsdb.save_db(ws_idx=idx_list[int(weak_idx2)], label_type=0)
                        bwsdb.save_log(ws_idx=idx_list[int(weak_idx2)], q_idx= question_idx, label_type=0)
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)
                    except:
                        strong_idx2 = request.form['radioOpt2']
                        bwsdb.save_db(ws_idx=idx_list[int(strong_idx2)], label_type=1)
                        bwsdb.save_log(ws_idx=idx_list[int(strong_idx2)], q_idx=question_idx, label_type=1)
                        checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
                        if checked == 1: 
                            weak_idx = idx_list.index(checked_idx)
                        if checked2 == 1:
                            strong_idx = idx_list.index(checked_idx2)
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
    bwsdb.connect()
    bwsdb.get_bws()
    bwsdb.get_bws_set()
    bwsdb.get_set_idx()
    bwsdb.table_to_csv(bwsdb.bws, 0)
    bwsdb.table_to_csv(bwsdb.bws_set, 1)
    
    if request.method == 'POST':
        try:
            idx = request.form['form_test']   # 1~8 
            set_idx = int(idx)
            bwsdb.save_set_idx(set_idx)
        except:
            try:
                request.form['check_annot']
                annot_check = 0
                bwsdb.get_bws_set_log()
                bwsdb.table_to_csv(bwsdb.bws_set_log, 2)
                bws_set_log_df = bwsdb.bws_set_log_df
                select_set = bws_set_log_df[bws_set_log_df.Set_no==bwsdb.set_idx]
                select_set.reset_index(inplace=True, drop=True)
                set_a = select_set[select_set.Weak_checked==0].index.tolist()
                set_b = select_set[select_set.Strong_checked==0].index.tolist()
                not_tagged = list(set(set_a + set_b)) 
                # print(not_tagged)    
            except:
                print('result do not work')
    return render_template('main.html', annot_check=annot_check, set_idx=bwsdb.set_idx, not_tagged=not_tagged)

def main():
    # app.run(debug=True, port=9509)
    app.run(host="192.168.123.110", debug=True, port=9509)
    
if __name__ == '__main__':
    global cli_argse 
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config_path", type=str, default='config')
    cli_parser.add_argument("--db_config", type=str, default='db_config.json')
    cli_argse = cli_parser.parse_args()
    main()