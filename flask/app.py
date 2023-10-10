# https://15xu0h4j6i.execute-api.us-east-2.amazonaws.com/dev

from flask import Flask, render_template
from flask import Flask, request, url_for

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['TEMPLATES_AUTO_RELOAD'] = True

import boto3
from datetime import datetime
from datetime import date
from io import StringIO
import math
import numpy as np
import os
import pandas as pd
import sys
import gviz_api
import json


## Functions ##
def data_ingest(file_name='static/22_23_wp_final_results.csv'):
	df = pd.read_csv(file_name)
	df['TEAM'] = df['TEAM'].str.title()
	
	return df

def aws_ingest(filename='22_23_wp_final_results.csv'):
	aws_id = os.getenv("aws_id")
	aws_secret = os.getenv("aws_secret")
	client = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
	# client = boto3.client('s3', aws_access_key_id='AKIAUJGHR52YIFCXQEUO', aws_secret_access_key='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')

	bucket_name = 'nba-wp'

	csv_obj = client.get_object(Bucket=bucket_name, Key=filename)
	body = csv_obj['Body']
	csv_string = body.read().decode('utf-8')

	df = pd.read_csv(StringIO(csv_string))

	return df


# def df_currentDate_operations(df):
# 	# reduce to current date
# 	df_current = (df['date'] == df['date'].max())
# 	df_current = df[df_current]
# 	del df_current['date']
# 	df_current = df_current.reset_index(drop=True)
# 	df_current.index += 1

# 	df_current.insert(0, 'id', range(1, 1 + len(df_current)))

# 	# column names
# 	return df_current


## Hard Codes ##
# df_columns_ind0 = ['TEAM','WT','LT','TIE_PC','WP','PT_DIFF','EXPECTED_WIN','EXPECTED_WP','WT_v_WP','WT_v_EXP_WP']
# url_p1 = 'https://cleaningtheglass.com/stats/team/'
# url_p2 = '#tab-offensive_overview'
# player_var = 'Player'
# age_var = 'Age'
# pos_var = 'Pos'
# gp_var = 'GP'
# min_var = 'MIN'
# mpg_var = 'MPG'
# usg_var = 'Usage'
# positions = ['Point','Combo','Wing','Forward','Big']


## Main Process ##
# Flask application
def main(filename='22_23_wp_final_results.csv'):

	df_final = aws_ingest(filename)
	# df_final = data_ingest(filename)

	# Cleanse for current averages
	# df_current = df_currentDate_operations(df_final)

	# df_current = df_current.round(1)
	# df_final = df_final.round(1)

	return df_final

## Main Execution ##
@app.route('/', methods=['GET','POST'])
def index():
	df = main()
	# sort = request.args.get('sort', 'Team_Name')
	# reverse = (request.args.get('direction', 'asc') == 'desc')
	# df = df.sort_values(by=[sort], ascending=reverse)

	table_description = {"TEAM": ("string", "Team Name"),
						"WT": ("number", "Winning Time %"),
						"LT": ("number", "Losing Time %"),
						"TIE_PC": ("number", "Tie Time %"),
						"WP": ("number", "Winning %"),
						"PT_DIFF": ("number", "Point Diff"),
						"EXPECTED_WIN": ("number", "Expected Wins"),
						"EXPECTED_WP": ("number", "Expected WP%"),
						"WT_v_WP": ("number", "WT% vs WP%"),
						"WT_v_EXP_WP": ("number", "WT% vs Expected WP%")
						}
	
	data_table = gviz_api.DataTable(table_description)
	table_data = df.to_dict(orient='records')	
	data_table.LoadData(table_data)
	json_table = data_table.ToJSon(columns_order=("TEAM",'WT','LT', 'TIE_PC',"WP","PT_DIFF","EXPECTED_WIN","EXPECTED_WP","WT_v_WP","WT_v_EXP_WP"))

	today = date.today()
	update_date = today.strftime("%m/%d/%Y")

	context = {"update_date": update_date}

	return render_template('wp_table.html',  table=json_table, context=context)


if __name__ == "__main__":
	app.run(debug=True)


