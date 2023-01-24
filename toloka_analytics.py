import pandas as pd
import requests
import toloka.client as toloka
import datetime
import json

e = datetime.datetime.now()
date = f"{'%s-%s-%s' % (e.year, e.month, e.day)}"

day = int(e.day) + 1
print(day)
new_day_plus = day - 4
start_time = '1672531200'
finish_time = str(1672790400 + new_day_plus * 86400)

URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

list_of_projects = []

list_of_pools = []

# COLLECT POOLS FROM PROJECT_ID LIST
for project_id in list_of_projects:
    success = False
    tries = 0
    while success != True:
        try:
            for status in ['OPEN']:
                r = requests.get(f'https://toloka.dev/api/v1/pools?status={status}&project_id={project_id}',
                                 headers=HEADERS).json()
                print('Проект: ', project_id)
                for pool in r['items']:
                    list_of_pools.append(pool['id'])
                    print(pool['id'])
                success = True
        except Exception as e:
            # print(e)
            tries += 1
            if tries == 10:
                success = True
            print('Account change')
            if OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            elif OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
            toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

print(list_of_pools)

full_df_toloka = pd.DataFrame()

# COLLECT ALL ASSIGNMENTS TO ONE DF
for pool_id in list_of_pools:
    success = False
    tries = 0
    print(pool_id)
    while success != True:
        try:
            df_toloka = toloka_client.get_assignments_df(pool_id, status=['SUBMITTED'])
            full_df_toloka = pd.concat([full_df_toloka, df_toloka])
            success = True
        except Exception as e:
            print(e)
            tries += 1
            if tries == 10:
                success = True
            print('Account change')
            if OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            elif OAUTH_TOKEN == '':
                OAUTH_TOKEN = ''
            HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
            toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

full_df_toloka['ASSIGNMENT:link'] = full_df_toloka['ASSIGNMENT:link'].apply(lambda x: x.replace('https://', '').split('/')[2])
full_df_toloka['ASSIGNMENT:started'] = full_df_toloka['ASSIGNMENT:started'].apply(lambda x: x.split('T')[0])

df = pd.DataFrame()

list_of_dates = []

# CREATE NEW DF WITH POOLS AND DATES
for i in range(1, 32):
    list_of_dates.append(f'{i}.01.2023')

df['DATE'] = list_of_dates

list_of_pools_with_statuses = []

# CREATE NEW COLUMNS FOR EVERY POOL
for pool in list_of_pools:
    list_of_pools_with_statuses.append(str(pool) + '_' + 'SUBMITTED')
    list_of_pools_with_statuses.append(str(pool) + '_' + 'ACCEPTED')
    list_of_pools_with_statuses.append(str(pool) + '_' + 'REJECTED')

for pool in list_of_pools_with_statuses:
    df[pool] = None

for pool in list_of_pools:
    df[str(pool) + '_' + 'STOCK'] = None

for pool in list_of_pools:
    df[str(pool) + '_' + 'OLDEST'] = None

# GET DATA FROM TOLOKA
for pool_id in list_of_pools:
    url = f'https://toloka.yandex.ru/api/stats/charts?chartTypes=POOL_SUBMIT_LINE_CHART%2CPOOL_APPROVE_LINE_CHART%2CPOOL_EXPIRE_LINE_CHART%2CPOOL_SKIP_LINE_CHART%2CPOOL_REJECT_LINE_CHART&intervalUnit=DAY&eventTimeFrom={start_time}&eventTimeTo={finish_time}&entityId={pool_id}&entityType=POOL'
    r = requests.get(url, headers=HEADERS)
    r = json.loads(r.content)
    print(r)
    sub_list = []
    appr_list = []
    reject_list = []

    for pool_stats in r['values']:
        pool_stats_name = pool_stats['type'].replace('POOL_', '').replace('_LINE_CHART', '')
        pool_stats_points = pool_stats['points']
        if pool_stats_name == 'SUBMIT' or pool_stats_name == 'APPROVE' or pool_stats_name == 'REJECT':
            print(pool_stats)
            print(pool_stats_name)
            print(pool_stats_points)
            if pool_stats_name == 'SUBMIT':
                sub_list = pool_stats_points
            elif pool_stats_name == 'APPROVE':
                appr_list = pool_stats_points
            elif pool_stats_name == 'REJECT':
                reject_list = pool_stats_points

    SUBMITTED = 0

    for assignment_status in full_df_toloka[full_df_toloka['ASSIGNMENT:link'] == pool_id]['ASSIGNMENT:status']:
        if assignment_status == 'SUBMITTED':
            SUBMITTED += 1

    df.loc[(df['DATE'] == '1.01.2023'), str(pool_id) + '_' + 'STOCK'] = SUBMITTED

    oldest_set = full_df_toloka[full_df_toloka['ASSIGNMENT:link'] == pool_id]['ASSIGNMENT:started'].min()

    df.loc[(df['DATE'] == '1.01.2023'), str(pool_id) + '_' + 'OLDEST'] = oldest_set

    for i in range(len(sub_list)):
        date_for_df = i + 1
        print(date_for_df)
        if not sub_list[i] == None:
            sub = int(sub_list[i])
        else:
            sub = 0
        if not appr_list[i] == None:
            appr = int(appr_list[i])
        else:
            appr = 0
        if not reject_list[i] == None:
            reject = int(reject_list[i])
        else:
            reject = 0
        df.loc[(df['DATE'] == f'{date_for_df}.01.2023'), f'{pool_id}_SUBMITTED'] = sub
        df.loc[(df['DATE'] == f'{date_for_df}.01.2023'), f'{pool_id}_ACCEPTED'] = appr
        df.loc[(df['DATE'] == f'{date_for_df}.01.2023'), f'{pool_id}_REJECTED'] = reject

new_df = pd.DataFrame()

new_df['DATE'] = df['DATE']

# CONCAT COLUMNS TO ONE MAIN
new_df['latino_SUBMITTED'] = df['36771671_SUBMITTED'] + df['36771652_SUBMITTED']
new_df['latino_REJECTED'] = df['36771671_REJECTED'] + df['36771652_REJECTED']
new_df['latino_ACCEPTED'] = df['36771671_ACCEPTED'] + df['36771652_ACCEPTED']

new_df['ME_SUBMITTED'] = df['36729901_SUBMITTED'] + df['36770485_SUBMITTED']
new_df['ME_REJECTED'] = df['36729901_REJECTED'] + df['36770485_REJECTED']
new_df['ME_ACCEPTED'] = df['36729901_ACCEPTED'] + df['36770485_ACCEPTED']

new_df['europe_SUBMITTED'] = df['36770747_SUBMITTED'] + df['36770901_SUBMITTED']
new_df['europe_REJECTED'] = df['36770747_REJECTED'] + df['36770901_REJECTED']
new_df['europe_ACCEPTED'] = df['36770747_ACCEPTED'] + df['36770901_ACCEPTED']

new_df['снг_SUBMITTED'] = df['36769670_SUBMITTED'] + df['36770250_SUBMITTED']
new_df['снг_REJECTED'] = df['36769670_REJECTED'] + df['36770250_REJECTED']
new_df['снг_ACCEPTED'] = df['36769670_ACCEPTED'] + df['36770250_ACCEPTED']

new_df['SA_SUBMITTED'] = df['36729896_SUBMITTED'] + df['36770583_SUBMITTED']
new_df['SA_REJECTED'] = df['36729896_REJECTED'] + df['36770583_REJECTED']
new_df['SA_ACCEPTED'] = df['36729896_ACCEPTED'] + df['36770583_ACCEPTED']

new_df['EA_SUBMITTED'] = df['36729893_SUBMITTED'] + df['36770540_SUBMITTED']
new_df['EA_REJECTED'] = df['36729893_REJECTED'] + df['36770540_REJECTED']
new_df['EA_ACCEPTED'] = df['36729893_ACCEPTED'] + df['36770540_ACCEPTED']

new_df['africa_SUBMITTED'] = df['36729845_SUBMITTED'] + df['36770346_SUBMITTED']
new_df['africa_REJECTED'] = df['36729845_REJECTED'] + df['36770346_REJECTED']
new_df['africa_ACCEPTED'] = df['36729845_ACCEPTED'] + df['36770346_ACCEPTED']


new_df['latino_STOCK'] = df['36771671_STOCK'] + df['36771652_STOCK']
new_df['ME_STOCK'] = df['36729901_STOCK'] + df['36770485_STOCK']
new_df['europe_STOCK'] = df['36770747_STOCK'] + df['36770901_STOCK']
new_df['снг_STOCK'] = df['36769670_STOCK'] + df['36770250_STOCK']
new_df['SA_STOCK'] = df['36729896_STOCK'] + df['36770583_STOCK']
new_df['EA_STOCK'] = df['36729893_STOCK'] + df['36770540_STOCK']
new_df['africa_STOCK'] = df['36729845_STOCK'] + df['36770346_STOCK']


new_df['latino_OLDEST'] = min([df['36771671_OLDEST'][0], df['36771652_OLDEST'][0]])
new_df['ME_OLDEST'] = min([df['36729901_OLDEST'][0], df['36770485_OLDEST'][0]])
new_df['europe_OLDEST'] = min([df['36770747_OLDEST'][0], df['36770901_OLDEST'][0]])
new_df['снг_OLDEST'] = min([df['36769670_OLDEST'][0], df['36770250_OLDEST'][0]])
new_df['SA_OLDEST'] = min([df['36729896_OLDEST'][0], df['36770583_OLDEST'][0]])
new_df['EA_OLDEST'] = min([df['36729893_OLDEST'][0], df['36770540_OLDEST'][0]])
new_df['africa_OLDEST'] = min([df['36729845_OLDEST'][0], df['36770346_OLDEST'][0]])



new_df.to_excel('statistics.xlsx', index=False)