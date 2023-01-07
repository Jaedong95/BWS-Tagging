import os 
import pymysql
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
    print(f'{set_idx}번 BWS Set 작업 중..')
    select_set = bws_set_df[bws_set_df.Set_no==set_idx]
    select_set.reset_index(inplace=True, drop=True)
    question_idx = (set_idx-1) * 400 + (question_no-1) 
    # print(f'해당 질문지 내용: {bws_set_df.loc[question_idx]}')
    idx_list = list(map(int, select_set.iloc[question_no - 1, 3:7]))
    # print(idx_list)
    txt_list = bws_df.loc[idx_list].text_kor.values.tolist()
    checked, checked2, checked_idx, checked_idx2 = bwsdb.is_checked(q_idx=question_idx)
    weak_idx = 9999
    strong_idx = 9999
    if checked == 1: 
        weak_idx = idx_list.index(checked_idx)
    if checked2 == 1:
        strong_idx = idx_list.index(checked_idx2)
    if question_no == 1:
        if request.method == 'POST':
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
        return render_template('index_s.html', question_no=question_no, idx_list=idx_list, txt_list=txt_list, checked=checked, checked2=checked2, weak_idx=weak_idx, strong_idx=strong_idx)
    elif question_no == 400:
        if request.method == 'POST':
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
    global bwsdb
    if request.method == 'POST':
        global set_idx 
        idx = request.form['form_test']   # 1~8 
        set_idx = int(idx)

    with open(os.path.join(cli_argse.config_path, cli_argse.db_config)) as f:
        args = AttrDict(json.load(f))
    bwsdb = BWSDB(args)
    bwsdb.connect()
    bwsdb.get_bws()
    bwsdb.get_bws_set()
    bwsdb.table_to_csv(bwsdb.bws, 0)
    bwsdb.table_to_csv(bwsdb.bws_set, 1)
    return render_template('main.html')

def main():
    app.run()

if __name__ == '__main__':
    global cli_argse 
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config_path", type=str, default='config')
    cli_parser.add_argument("--db_config", type=str, default='db_config.json')
    cli_argse = cli_parser.parse_args()
    main()