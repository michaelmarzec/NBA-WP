## AWS Prep ##
import json 
import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('nba-wp')

## Imports ##
from bs4 import BeautifulSoup
import datetime
import numpy as np
import pandas as pd
import requests

# from botocore.vendored import requests
# import requests

from time import sleep
from urllib.request import urlopen

# try:
#     from utils import get_game_suffix
# except:
#     from basketball_reference_scraper.utils import get_game_suffix

## Functions ##

# Data Prep #
# Extract game log (modified version for updates to the basketball_reference_scraper library)
def get_game_suffix(date, team1, team2):
    r = requests.get(f'https://www.basketball-reference.com/boxscores/index.fcgi?year={date.year}&month={date.month}&day={date.day}')
    suffix = None
    if r.status_code==200:
        soup = BeautifulSoup(r.content, 'html.parser')
        for table in soup.find_all('table', attrs={'class': 'teams'}):
            for anchor in table.find_all('a'):
                if 'boxscores' in anchor.attrs['href']:
                    if team1 in anchor.attrs['href'] or team2 in anchor.attrs['href']:
                        suffix = anchor.attrs['href']
    return suffix
    
def get_pbp_helper(suffix):
    selector = f'#pbp'
    r = requests.get(f'https://www.basketball-reference.com/boxscores/pbp{suffix}')
    if r.status_code==200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table', attrs={'id': 'pbp'})
        return pd.read_html(str(table), flavor="lxml")[0]

def format_df(df1):
    df1.columns = list(map(lambda x: x[1], list(df1.columns)))
    t1 = list(df1.columns)[1].upper()
    t2 = list(df1.columns)[5].upper()
    q = 1
    df = None
    for index, row in df1.iterrows():
        d = {'QUARTER': float('nan'), 'TIME_REMAINING': float('nan'), f'{t1}_ACTION': float('nan'), f'{t2}_ACTION': float('nan'), f'{t1}_SCORE': float('nan'), f'{t2}_SCORE': float('nan')}
        if row['Time']=='2nd Q':
            q = 2
        elif row['Time']=='3rd Q':
            q = 3
        elif row['Time']=='4th Q':
            q = 4
        elif 'OT' in row['Time']:
            q = row['Time'][0]+'OT'
        try:
            d['QUARTER'] = q
            d['TIME_REMAINING'] = row['Time']
            scores = row['Score'].split('-')
            d[f'{t1}_SCORE'] = int(scores[0])
            d[f'{t2}_SCORE'] = int(scores[1])
            d[f'{t1}_ACTION'] = row[list(df1.columns)[1]]
            d[f'{t2}_ACTION'] = row[list(df1.columns)[5]]
            if df is None:
                df = pd.DataFrame(columns = list(d.keys()))
            df = pd.concat([df, pd.DataFrame(d, index=[0])], ignore_index=True)#df.append(d, ignore_index=True)
        except:
            continue
    return df

def get_pbp(date, team1, team2):
    date = pd.to_datetime(date)
    suffix = get_game_suffix(date, team1, team2).replace('/boxscores', '')
    df = get_pbp_helper(suffix)
    df = df.iloc[1:].reset_index(drop=True)
    df = format_df(df)
    return df

# calculate winning time %
def calculate_game_wp(pbp_df, date, away_team, home_team):
    pbp_df = pbp_df.rename(columns={
        pbp_df.columns[4]:"AWAY_SCORE",
        pbp_df.columns[5]:"HOME_SCORE"
    })

    pbp_df = pbp_df[['QUARTER','TIME_REMAINING','AWAY_SCORE','HOME_SCORE']]
    pbp_df['TIME_REMAINING'] = pbp_df.TIME_REMAINING.replace('24:00.0','12:00.0')
    pbp_df['TIME_REMAINING'] = pd.to_datetime(pbp_df['TIME_REMAINING'])
    pbp_df['TIME_REMAINING'] = pd.to_datetime(pbp_df['TIME_REMAINING'],format= '%H:%M:%S').dt.time
    pbp_df['AWAY_SCORE'] = pbp_df['AWAY_SCORE'].astype(int)
    pbp_df['HOME_SCORE'] = pbp_df['HOME_SCORE'].astype(int)

    quarter_beginning = '12:00:00'
    quarter_ending = '00:00:00'
    overtime_beginning = '05:00:00'

    list_of_quarters = list(pbp_df['QUARTER'].unique())

    full_df = pd.DataFrame()
    for quarter in list_of_quarters:
        quarter_df = pbp_df[pbp_df['QUARTER'] == quarter]

        if quarter == 1:# add beginning of 1st quarter
            quarter_df.loc[-1] = [1, quarter_beginning, 0, 0] 
            quarter_df.index = quarter_df.index + 1  
            quarter_df = quarter_df.sort_index()

            end_row = {
                'QUARTER': [quarter],
                'TIME_REMAINING': quarter_ending,
                'AWAY_SCORE': [quarter_df.iloc[-1,2]],
                'HOME_SCORE': [quarter_df.iloc[-1,3]]
            }

            end_row = pd.DataFrame(data=end_row)
            quarter_df = pd.concat([quarter_df, end_row], axis=0).reset_index(drop=True)

            temp_date = str(datetime.datetime.strptime('1900-01-01', '%Y-%m-%d').date())
            quarter_df['TIME_REMAINING'] = pd.to_datetime(temp_date + " " + quarter_df.TIME_REMAINING.astype(str))
            quarter_df['TIME_ELAPSED'] = (quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))
            quarter_df['INDICATOR'] = np.where(
                quarter_df['QUARTER'] == quarter_df['QUARTER'].shift(-1),
                ((quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))/60000000000).view(int),
                0
            )

            full_df = pd.concat([full_df, quarter_df], axis=0)


        elif quarter in (2,3,4):
            start_row = end_row
            start_row['QUARTER'] = quarter
            start_row['TIME_REMAINING'] = quarter_beginning

            quarter_df = quarter_df.reset_index(drop=True)
            quarter_df.index += 1

            quarter_df = pd.concat([start_row, quarter_df],axis=0)
            quarter_df = quarter_df.sort_index()

            end_row = {
                'QUARTER': [quarter],
                'TIME_REMAINING': quarter_ending,
                'AWAY_SCORE': [quarter_df.iloc[-1,2]],
                'HOME_SCORE': [quarter_df.iloc[-1,3]]
            }

            end_row = pd.DataFrame(data=end_row)
            quarter_df = pd.concat([quarter_df, end_row], axis=0).reset_index(drop=True)

            temp_date = str(datetime.datetime.strptime('1900-01-01', '%Y-%m-%d').date())
            quarter_df['TIME_REMAINING'] = pd.to_datetime(temp_date + " " + quarter_df.TIME_REMAINING.astype(str))
            quarter_df['TIME_ELAPSED'] = (quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))
            quarter_df['INDICATOR'] = np.where(
                quarter_df['QUARTER'] == quarter_df['QUARTER'].shift(-1),
                ((quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))/60000000000).view(int),
                0
            )

            full_df = pd.concat([full_df, quarter_df], axis=0)

        else:
            start_row = end_row
            start_row['QUARTER'] = quarter
            start_row['TIME_REMAINING'] = overtime_beginning

            quarter_df = quarter_df.reset_index(drop=True)
            quarter_df.index += 1

            quarter_df = pd.concat([start_row, quarter_df],axis=0)
            quarter_df = quarter_df.sort_index()

            end_row = {
                'QUARTER': [quarter],
                'TIME_REMAINING': quarter_ending,
                'AWAY_SCORE': [quarter_df.iloc[-1,2]],
                'HOME_SCORE': [quarter_df.iloc[-1,3]]
            }

            end_row = pd.DataFrame(data=end_row)
            quarter_df = pd.concat([quarter_df, end_row], axis=0).reset_index(drop=True)

            temp_date = str(datetime.datetime.strptime('1900-01-01', '%Y-%m-%d').date())
            quarter_df['TIME_REMAINING'] = pd.to_datetime(temp_date + " " + quarter_df.TIME_REMAINING.astype(str))
            quarter_df['TIME_ELAPSED'] = (quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))
            quarter_df['INDICATOR'] = np.where(
                quarter_df['QUARTER'] == quarter_df['QUARTER'].shift(-1),
                ((quarter_df['TIME_REMAINING'] - quarter_df['TIME_REMAINING'].shift(-1))/60000000000).view(int),
                0
            )

            full_df = pd.concat([full_df, quarter_df], axis=0)

    full_df['LEADER'] = np.where(
        full_df.HOME_SCORE > full_df.AWAY_SCORE,
        'HOME_TEAM',
        'AWAY_TEAM',
    )

    full_df['LEADER'] = np.where(
        full_df.HOME_SCORE == full_df.AWAY_SCORE,
        'NEITHER',
        full_df.LEADER
    )
    full_df_gb = full_df.groupby('LEADER')['INDICATOR'].sum().reset_index() 

    teams = ['AWAY_TEAM','HOME_TEAM']
    total_seconds = full_df_gb.INDICATOR.sum()

    results_row = pd.DataFrame(columns=['DATE','AWAY_TEAM','HOME_TEAM','AWAY_WT','HOME_WT'],index=[0])
    results_row['DATE'] = date
    results_row['AWAY_TEAM'] = away_team
    results_row['HOME_TEAM'] = home_team
    
    try:
        away_seconds_leading = full_df_gb[full_df_gb.LEADER == 'AWAY_TEAM'].INDICATOR.values[0]
    except:
        away_seconds_leading = 0
    away_wp = away_seconds_leading / total_seconds

    try:
        home_seconds_leading = full_df_gb[full_df_gb.LEADER == 'HOME_TEAM'].INDICATOR.values[0]
    except:
        home_seconds_leading = 0
    home_wp = home_seconds_leading / total_seconds
    
    results_row['AWAY_WT'] = away_wp
    results_row['HOME_WT'] = home_wp

    return results_row

    
# function to calculate team average for each metric
def calculate_tm_avg_metric(df, metric):
    # set metric columns
    if metric in ('WT','LT'):
        away_col = 'AWAY_' + metric
        home_col = 'HOME_' + metric
    elif metric == 'TIE_PC':
        away_col = metric
        home_col = metric
    
    # reformat into single column
    nba_results_22_23_away = df[['DATE','AWAY_TEAM', away_col]].rename(columns={'AWAY_TEAM':'TEAM',away_col:metric})
    nba_results_22_23_home = df[['DATE','HOME_TEAM', home_col]].rename(columns={'HOME_TEAM':'TEAM',home_col:metric})
        
        

    # concatenate
    nba_results_22_23_reformat = pd.concat([nba_results_22_23_away, nba_results_22_23_home]).reset_index(drop=True)
    
    # find team averages
    nba_results_22_23_agg = nba_results_22_23_reformat.groupby(['TEAM']).mean().reset_index()
    return nba_results_22_23_agg



## Main Process ##
def main(wt_results_file, nba_schedule_file, output_file):
    # read CSVs
    nba_wt_results_df = pd.read_csv(wt_results_file)
    nba_wt_results_df['DATE'] = pd.to_datetime(nba_wt_results_df['DATE'])

    nba_schedule_df = pd.read_csv(nba_schedule_file)
    nba_schedule_df['DATE'] = pd.to_datetime(nba_schedule_df['DATE'])

    # nba_schedule_df_retro = nba_schedule_df[
    #   (nba_schedule_df.DATE>='2023-10-24') &
    #   (nba_schedule_df.DATE<datetime.date.today().strftime('%Y-%m-%d'))
    # ].reset_index(drop=True)

    # reduce to yesterday's results
#   nba_schedule_df_retro = nba_schedule_df[
#       nba_schedule_df.DATE == ((datetime.date.today() - datetime.timedelta(1)).strftime('%Y-%m-%d'))
#   ].reset_index(drop=True)
    nba_schedule_df_retro = nba_schedule_df[
        nba_schedule_df.DATE == '2023-10-31'
    ].reset_index(drop=True)

    nba_wt_results_yesterday = pd.DataFrame()

    # extract new results
    for game_number in range(len(nba_schedule_df_retro)):
        if (game_number % 9) == 0:
            sleep(120)
        date = nba_schedule_df_retro.loc[game_number, "DATE"]
        away_team = nba_schedule_df_retro.loc[game_number, "AWAY_TEAM"]
        home_team = nba_schedule_df_retro.loc[game_number, "HOME_TEAM"]

        pbp_df = get_pbp(date, away_team, home_team)
        # print(str(date) + '-' + str(game_number) + '-' + away_team + '-' + home_team)
        wp_results = calculate_game_wp(pbp_df, date, away_team, home_team)
        nba_wt_results_yesterday = pd.concat([nba_wt_results_yesterday, wp_results]).reset_index(drop=True)
    
    # concat into one df
    nba_wt_results_df = pd.concat([nba_wt_results_df, nba_wt_results_yesterday])
    nba_wt_results_df.to_csv('wt_support/23_24_wt_results.csv', index=False)

    # conduct aggregations
    # calculate 2022-23 LT% and TIE%
    nba_wt_results_df['AWAY_LT'] = nba_wt_results_df['HOME_WT']
    nba_wt_results_df['HOME_LT'] = nba_wt_results_df['AWAY_WT']
    nba_wt_results_df['HOME_LT'] = nba_wt_results_df['AWAY_WT']
    nba_wt_results_df['TIE_PC'] = 1 - (nba_wt_results_df['AWAY_WT'] + nba_wt_results_df['HOME_WT'])

    # create datatset of wt/lt/tie%
    wt_results = calculate_tm_avg_metric(nba_wt_results_df, 'WT')
    lt_results = calculate_tm_avg_metric(nba_wt_results_df, 'LT')
    tie_results = calculate_tm_avg_metric(nba_wt_results_df, 'TIE_PC')

    del wt_results['DATE']
    del lt_results['DATE']
    del tie_results['DATE'] 

    nba_results_agg = pd.merge(wt_results, lt_results, how='inner', on='TEAM')
    nba_results_agg = pd.merge(nba_results_agg, tie_results, how='inner', on='TEAM')

    # optional validation that data is accurate
    # print((nba_results_22_23_agg['WT'] + nba_results_22_23_agg['LT'] + nba_results_22_23_agg['TIE_PC']).min())
    # print((nba_results_22_23_agg['WT'] + nba_results_22_23_agg['LT'] + nba_results_22_23_agg['TIE_PC']).max())

    ## things to extract (from cleaningtheglass)
    # WP (wins/loss), #PT_DIFF, #Expected_WIN
    html = urlopen('https://cleaningtheglass.com/stats/league/summary')
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find("table")
    df_ctg = pd.read_html(str(table), flavor="lxml")[0]
    df_ctg.columns = df_ctg.columns.get_level_values(1)
    df_ctg = df_ctg.iloc[: , 1:]
    df_ctg = df_ctg.iloc[: , :7]
    df_ctg_1 = df_ctg['Team']
    df_ctg_2 = df_ctg.iloc[: , 2]
    df_ctg_3 = df_ctg.iloc[: , 5:]
    df_ctg = pd.concat([df_ctg_1, df_ctg_2, df_ctg_3], axis=1)

    team_names = {
        'full_name':[
            'Atlanta',
            'Boston',
            'Brooklyn',
            'Charlotte',
            'Chicago',
            'Cleveland',
            'Dallas',
            'Denver',
            'Detroit',
            'Golden State',
            'Houston',
            'Indiana',
            'LA Clippers',
            'LA Lakers',
            'Memphis',
            'Miami',
            'Milwaukee',
            'Minnesota',
            'New Orleans',
            'New York',
            'Oklahoma City',
            'Orlando',
            'Philadelphia',
            'Phoenix',
            'Portland',
            'Sacramento',
            'San Antonio',
            'Toronto',
            'Utah',
            'Washington'
            ],
        'abbr_name':[
            'ATL',
            'BOS',
            'BRK',
            'CHO',
            'CHI',
            'CLE',
            'DAL',
            'DEN',
            'DET',
            'GSW',
            'HOU',
            'IND',
            'LAC',
            'LAL',
            'MEM',
            'MIA',
            'MIL',
            'MIN',
            'NOP',
            'NYK',
            'OKC',
            'ORL',
            'PHI',
            'PHO',
            'POR',
            'SAC',
            'SAS',
            'TOR',
            'UTA',
            'WAS'
            ]
    }

    team_name_map = pd.DataFrame(team_names)

    df_ctg = pd.merge(df_ctg, team_name_map, how='inner', left_on ='Team', right_on='full_name')
    del df_ctg['Team']
    del df_ctg['full_name']
    df_ctg = df_ctg.rename(columns={
        'abbr_name':'TEAM',
        'Win%':'WP',
        'Exp W82':'EXPECTED_WIN',
        'Point Diff':'PT_DIFF'
    })

    df_ctg['WP'] = df_ctg['WP'].str.rstrip('%').astype('float') / 100.0
    df_ctg['EXPECTED_WP'] = df_ctg['EXPECTED_WIN'] / 82


    nba_wt_results_agg = pd.merge(nba_results_agg, df_ctg, how='inner', on='TEAM')
    nba_wt_results_agg['WT_v_WP'] = nba_wt_results_agg['WT'] - nba_wt_results_agg['WP']
    nba_wt_results_agg['WT_v_EXP_WP'] = nba_wt_results_agg['WT'] - nba_wt_results_agg['EXPECTED_WP']

    nba_wt_results_agg.to_csv(output_file)


def lambda_handler(event, context):

    bucket.download_file('23_24_nba_schedule.csv','/tmp/23_24_nba_schedule.csv')
    bucket.download_file('23_24_wt_results.csv','/tmp/23_24_wt_results.csv')
    
    main('/tmp/23_24_wt_results.csv', '/tmp/23_24_nba_schedule.csv', '/tmp/nba_wt_results_agg.csv')

    bucket.upload_file('/tmp/nba_wt_results_agg.csv', 'nba_wt_results_agg.csv')
    
    return {
        'message': 'CTG Download + Data Update = Success'
    }


