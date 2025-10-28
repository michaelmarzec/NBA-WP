# https://15xu0h4j6i.execute-api.us-east-2.amazonaws.com/dev

from flask import Flask, render_template
from flask import Flask, request, url_for

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True

import boto3
# from datetime import datetime
from datetime import date
from io import StringIO
# import math
# import numpy as np
import os
import pandas as pd
# import sys
import gviz_api
# import json

## Functions ##
def data_ingest(file_name='static/22_23_wp_final_results.csv'):
	df = pd.read_csv(file_name)
	df['TEAM'] = df['TEAM'].str.title()
	
	return df

def aws_ingest(filename='22_23_wp_final_results.csv'): 
	aws_id = os.getenv("aws_id")
	aws_secret = os.getenv("aws_secret")
	client = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)

	bucket_name = 'nba-wp'

	csv_obj = client.get_object(Bucket=bucket_name, Key=filename)
	body = csv_obj['Body']
	csv_string = body.read().decode('utf-8')

	df = pd.read_csv(StringIO(csv_string))
	df['WT'] = df['WT'] * 100
	df['LT'] = df['LT'] * 100
	df['TIE_PC'] = df['TIE_PC'] * 100
	df['WP'] = df['WP'] * 100
	df['EXPECTED_WP'] = df['EXPECTED_WP'] * 100
	df['WT_v_WP'] = df['WT_v_WP'] * 100
	df['WT_v_EXP_WP'] = df['WT_v_EXP_WP'] * 100

	return df

## Main Process ##
# Flask application
def main(filename='22_23_wp_final_results.csv'):

	df_final = aws_ingest(filename)

	return df_final

## Main Execution ##
@app.route('/', methods=['GET','POST'])
def index():
	df = main('nba_wt_results_agg.csv')

	table_description = {"TEAM": ("string", "Team Name"),
						"WT": ("number", "Winning Time %"),
						"LT": ("number", "Losing Time %"),
						"TIE_PC": ("number", "Tie Time %"),
						"Wins": ("number", "Wins"),
						"Losses": ("number", "Losses"),
						"TIE_PC": ("number", "Tie Time %"),
						"WP": ("number", "Win %"),
						"PT_DIFF": ("number", "Point Diff"),
						"EXPECTED_WIN": ("number", "Expected Wins"),
						"EXPECTED_WP": ("number", "Expected Win %"),
						"WT_v_WP": ("number", "Win Time % vs Win %"),
						"WT_v_EXP_WP": ("number", "Win Time % vs Expected Win %")
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("TEAM","WT","LT","TIE_PC","Wins","Losses","WP","PT_DIFF","EXPECTED_WIN","EXPECTED_WP","WT_v_WP","WT_v_EXP_WP"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('wp_table.html',  table=json_table, context=context)

@app.route('/24_25', methods=['GET', 'POST'])
def index_24_25():
	df = main('24_25_wp_final_results.csv')

	table_description = {"TEAM": ("string", "Team Name"),
						 "WT": ("number", "Winning Time %"),
						 "LT": ("number", "Losing Time %"),
						 "TIE_PC": ("number", "Tie Time %"),
						 "Wins": ("number", "Wins"),
						 "Losses": ("number", "Losses"),
						 "TIE_PC": ("number", "Tie Time %"),
						 "WP": ("number", "Win %"),
						 "PT_DIFF": ("number", "Point Diff"),
						 "EXPECTED_WIN": ("number", "Expected Wins"),
						 "EXPECTED_WP": ("number", "Expected Win %"),
						 "WT_v_WP": ("number", "Win Time % vs Win %"),
						 "WT_v_EXP_WP": ("number", "Win Time % vs Expected Win %")
						 }

	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=(
	"TEAM", "WT", "LT", "TIE_PC", "Wins", "Losses", "WP", "PT_DIFF", "EXPECTED_WIN", "EXPECTED_WP", "WT_v_WP",
	"WT_v_EXP_WP"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('wp_table_2425.html', table=json_table, context=context)

@app.route('/22_23', methods=['GET','POST'])
def index_22_23():
	df = main('22_23_wp_final_results.csv')

	table_description = {"TEAM": ("string", "Team Name"),
						"WT": ("number", "Winning Time %"),
						"LT": ("number", "Losing Time %"),
						"TIE_PC": ("number", "Tie Time %"),
						"Wins": ("number", "Wins"),
						"Losses": ("number", "Losses"),
						"TIE_PC": ("number", "Tie Time %"),
						"WP": ("number", "Win %"),
						"PT_DIFF": ("number", "Point Diff"),
						"EXPECTED_WIN": ("number", "Expected Wins"),
						"EXPECTED_WP": ("number", "Expected Win %"),
						"WT_v_WP": ("number", "Win Time % vs Win %"),
						"WT_v_EXP_WP": ("number", "Win Time % vs Expected Win %")
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("TEAM","WT","LT", "TIE_PC","Wins","Losses","WP","PT_DIFF","EXPECTED_WIN","EXPECTED_WP","WT_v_WP","WT_v_EXP_WP"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('wp_table_2223.html',  table=json_table, context=context)


@app.route('/23_24', methods=['GET', 'POST'])
def index_23_24():
	df = main('23_24_wp_final_results.csv')

	table_description = {"TEAM": ("string", "Team Name"),
						 "WT": ("number", "Winning Time %"),
						 "LT": ("number", "Losing Time %"),
						 "TIE_PC": ("number", "Tie Time %"),
						 "Wins": ("number", "Wins"),
						 "Losses": ("number", "Losses"),
						 "TIE_PC": ("number", "Tie Time %"),
						 "WP": ("number", "Win %"),
						 "PT_DIFF": ("number", "Point Diff"),
						 "EXPECTED_WIN": ("number", "Expected Wins"),
						 "EXPECTED_WP": ("number", "Expected Win %"),
						 "WT_v_WP": ("number", "Win Time % vs Win %"),
						 "WT_v_EXP_WP": ("number", "Win Time % vs Expected Win %")
						 }

	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=(
	"TEAM", "WT", "LT", "TIE_PC", "Wins", "Losses", "WP", "PT_DIFF", "EXPECTED_WIN", "EXPECTED_WP", "WT_v_WP",
	"WT_v_EXP_WP"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('wp_table_2324.html', table=json_table, context=context)

if __name__ == "__main__":
	app.run(debug=True)


