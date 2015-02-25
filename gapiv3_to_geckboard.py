# Bunny Inc Integration between GA and Geckoboard
"""
Before You Begin:
1.Update the client_secrets.json file
2.Supply your TABLE_ID
Sample Usage:
  $ python gapiv3_to_geckboard.py
"""
__author__ = 'ronald.bunnyinc.com (Ronald Angel)'
import argparse
import sys
from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
import time
from datetime import date, timedelta
import json
import mysql.connector



def get_goal_referers_list(table_id, goal_id):
    # Declare command-line flags.
    argparser = argparse.ArgumentParser(add_help=False)
    argv = []
    # Authenticate and construct service.
    service, flags = sample_tools.init(
        argv, 'analytics', 'v3', __doc__, __file__, parents=[argparser],
        scope='https://www.googleapis.com/auth/analytics.readonly')

    # Try to make a request to the API. Print the results or handle errors.
    try:
        results = get_query_best_referrers_last_week(service, table_id, goal_id).execute()
        export_geckoboard_list_results(results,goal_id)
    except TypeError, error:
        # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)

    except HttpError, error:
        # Handle API errors.
        print ('Arg, there was an API error : %s : %s' %
               (error.resp.status, error._get_reason()))

    except AccessTokenRefreshError:
        # Handle Auth errors.
        print ('The credentials have been revoked or expired, please re-run '
               'the application to re-authorize')


def get_query_best_referrers_last_week(service, table_id, goal_id):
    today = date.today()
    init_day = date.today() - timedelta(days=7)
    return service.data().ga().get(
        ids=table_id,
        start_date=init_day.isoformat(),
        end_date=today.isoformat(),
        dimensions='ga:source,ga:medium',
        metrics='ga:goal' + goal_id + 'Starts,ga:goal' + goal_id + 'Completions',
        sort='-ga:goal' + goal_id + 'Completions',
        start_index='1',
        max_results='25')


def export_geckoboard_list_results(results, goal_id):
    json_output = []
    cnx = mysql.connector.connect(user='ronald_angel', database='voice123', password='hawrosavel', host='rds.v123.lan.bunnyinc.com')
    cursor = cnx.cursor()
    if results.get('rows', []):
        for row in results.get('rows'):
            json_output.append({"title": {'text': "" + str(row[0])}, "label": {"name": "" + str(row[2]), "color": "#ff2015"}, 'description': "" + str(row[3])})
    print str(json_output)

    ts = time.time()
    with open('v123_goal'+goal_id+'.json', 'wb') as fp:
        json.dump(json_output, fp)

    add_integration = ("INSERT INTO integrations_v123"
               "(integration_name, answer_value, response_value, date_creation) "
               "VALUES (%s, %s,%s,%s)")

    data_integration = ('ga','v123_goal'+goal_id,str(json_output), date.today())
    cursor.execute(add_integration, data_integration)
    cnx.commit()
    print cursor.lastrowid



if __name__ == '__main__':
    get_goal_referers_list('ga:49452093', '1')
