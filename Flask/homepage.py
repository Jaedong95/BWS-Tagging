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
    idx_list = [] 
    idx_list.append(select_set.loc[question_no - 1].idx1)
    idx_list.append(select_set.loc[question_no - 1].idx2)
    idx_list.append(select_set.loc[question_no - 1].idx3)
    idx_list.append(select_set.loc[question_no - 1].idx4)
    txt_list = bws_df.loc[idx_list].text_kor.values.tolist()
    if question_no == 1:
        if request.method == 'POST':
            try:
                weak_idx = request.form['radioOpt1']
                print(f'weakest: {weak_idx}, {idx_list[int(weak_idx)]}')
                bwsdb.save_db(idx_list[int(weak_idx)], 0)
            except:
                strong_idx = request.form['radioOpt2']
                print(f'strongest: {strong_idx}, {idx_list[int(strong_idx)]}')
                bwsdb.save_db(idx_list[int(strong_idx)], 1)
        return render_template('index_s.html', question_no=question_no, txt_list=txt_list)
    elif question_no == 400:
        if request.method == 'POST':
            try:
                weak_idx = request.form['radioOpt1']
                print(f'weakest: {weak_idx}, {idx_list[int(weak_idx)]}')
                bwsdb.save_db(idx_list[int(weak_idx)], 0)
            except:
                strong_idx = request.form['radioOpt2']
                print(f'strongest: {strong_idx}', {idx_list[int(strong_idx)]})
                bwsdb.save_db(idx_list[int(strong_idx)], 1)
        return render_template('index_e.html', question_no=question_no, txt_list=txt_list)
    else: 
        if request.method == 'POST':
            try:
                weak_idx = request.form['radioOpt1']
                print(f'weakest: {weak_idx}, {idx_list[int(weak_idx)]}')
                bwsdb.save_db(0, ws_idx=idx_list[int(weak_idx)])
            except:
                strong_idx = request.form['radioOpt2']
                print(f'strongest: {strong_idx}, {idx_list[int(strong_idx)]}')
                bwsdb.save_db(1, ws_idx=idx_list[int(strong_idx)])
        return render_template('index.html', question_no=question_no, txt_list=txt_list)

@app.route('/', methods=['GET', 'POST'])
def main_page():
    global select_set
    global bwsdb
    if request.method == 'POST':
        global set_idx 
        idx = request.form['form_test']
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

def main(cli_argse):
    app.run()

if __name__ == '__main__':
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config_path", type=str, default='config')
    cli_parser.add_argument("--db_config", type=str, default='db_config.json')
    cli_argse = cli_parser.parse_args()
    main(cli_argse)