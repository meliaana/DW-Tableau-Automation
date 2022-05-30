import json
import uuid
import datetime
from dotenv import load_dotenv
import os
from dateutil.relativedelta import relativedelta
from datetime import date
import logging

import pyodbc as pyodbc

import pandas as pd
import requests
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, \
    TableName
import tableauserverclient as TSC

load_dotenv()
logging.basicConfig(filename=f"DWTab-{datetime.datetime.now}",
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

New_Hyper_name = 'last_3month_data_fromDW.hyper'

# Database credentials
db_server = os.getenv('db_server')
db_database = os.getenv('db_database')
db_username = os.getenv('db_username')
db_password = os.getenv('db_password')

# Tableau credentials
tableau_server_url = os.getenv('tableau_server_url')
tableau_token_name = os.getenv('tableau_token_name')
tableau_token_secret = os.getenv('tableau_token_secret')
tableau_site = os.getenv('tableau_site')

# Tableau project and datasource credentials
project_Id = os.getenv('project_Id')
old_datasource = os.getenv('old_datasource')
old_datasource_id = os.getenv('old_datasource_id')

three_month_ago = (date.today() + relativedelta(months=-3)).strftime("%m/%d/%Y")

tb_server = TSC.Server(tableau_server_url, use_server_version=True)
tb_tableau_auth = TSC.PersonalAccessTokenAuth(token_name=tableau_token_name
                                              , personal_access_token=tableau_token_secret
                                              , site_id=tableau_site)

old_datasource_item = TSC.DatasourceItem(project_Id, name=old_datasource)


def DataFromDW():
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_server + ';DATABASE=' + db_database + ';UID=' +
        db_username + ';PWD=' + db_password)
    logger.info('Connected to Database - ', datetime.datetime.now())
    # Fetch data from database
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    sql_query = f"SELECT * FROM vwFactBetMirrorDW WHERE VendorSettledDate >={three_month_ago}"
    cursor.execute(sql_query)
    logger.info('Executed script - ', sql_query, '-', datetime.datetime.now())
    df = pd.DataFrame()

    rows = 0
    while True:
        dat = cursor.fetchmany(8000)
        if not dat:
            break
        df = pd.concat([df, pd.DataFrame.from_records(dat, columns=cursor.description)])
        rows += df.shape[0]
    df.columns = ['Brand', 'Currency', 'VendorSettledDate', 'MemberCode', 'AffiliateCategoryName', 'IsVIP',
                  'AffiliateCode', 'registerDate', 'VIPLevel', 'RiskLevel', 'AccountStatus',
                  'HousePlayerStatus', 'FreeSpin', 'Game', 'GameGroup', 'GamePlatformGroup', 'Product',
                  'lifeCycleBackward', 'LifeCycleForward', 'PlayerPreferenceType', 'ProductPreference',
                  'DepositClassName', 'LifeTimeDepositClassName', 'LifetimeDepositGroupName', 'BetCount',
                  'WinLossAmount', 'TurnOverAmount', 'ComboBonusAmount']

    df['VendorSettledDate'] = df['VendorSettledDate'].apply(pd.to_datetime)
    df['registerDate'] = df['registerDate'].apply(pd.to_datetime)

    logger.info('Created dataframe - ', datetime.datetime.now())
    return df


def createHyperFromDF(hyperFileName, my_df):
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp') as hyper:
        # Create the the .hyper file, replace it if it already exists
        with Connection(endpoint=hyper.endpoint,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        database=hyperFileName) as connection:
            # Create the schema
            connection.catalog.create_schema('Extract')

            # Create the table definition
            schema = TableDefinition(table_name=TableName('Extract', 'Extract'),
                                     columns=[
                                         TableDefinition.Column('Brand', SqlType.text()),
                                         TableDefinition.Column('Currency', SqlType.text()),
                                         TableDefinition.Column('VendorSettledDate', SqlType.date()),
                                         TableDefinition.Column('MemberCode', SqlType.text()),
                                         TableDefinition.Column('AffiliateCategoryName', SqlType.text()),
                                         TableDefinition.Column('IsVIP', SqlType.text()),
                                         TableDefinition.Column('AffiliateCode', SqlType.text()),
                                         TableDefinition.Column('registerDate', SqlType.date()),
                                         TableDefinition.Column('VIPLevel', SqlType.text()),
                                         TableDefinition.Column('RiskLevel', SqlType.text()),
                                         TableDefinition.Column('AccountStatus', SqlType.text()),
                                         TableDefinition.Column('HousePlayerStatus', SqlType.text()),
                                         TableDefinition.Column('FreeSpin', SqlType.text()),
                                         TableDefinition.Column('Game', SqlType.text()),
                                         TableDefinition.Column('GameGroup', SqlType.text()),
                                         TableDefinition.Column('GamePlatformGroup', SqlType.text()),
                                         TableDefinition.Column('Product', SqlType.text()),
                                         TableDefinition.Column('lifeCycleBackward', SqlType.text()),
                                         TableDefinition.Column('LifeCycleForward', SqlType.text()),
                                         TableDefinition.Column('PlayerPreferenceType', SqlType.text()),
                                         TableDefinition.Column('ProductPreference', SqlType.text()),
                                         TableDefinition.Column('DepositClassName', SqlType.text()),
                                         TableDefinition.Column('LifeTimeDepositClassName', SqlType.text()),
                                         TableDefinition.Column('LifetimeDepositGroupName', SqlType.text()),
                                         TableDefinition.Column('BetCount', SqlType.int()),
                                         TableDefinition.Column('WinLossAmount', SqlType.double()),
                                         TableDefinition.Column('TurnOverAmount', SqlType.double()),
                                         TableDefinition.Column('ComboBonusAmount', SqlType.double()),

                                     ])

            # Create the table in the connection catalog
            connection.catalog.create_table(schema)
            logger.info('Created hyper file - ', datetime.datetime.now())

            # Add data to .hyper file
            with Inserter(connection, schema) as inserter:
                for index, row in my_df.iterrows():
                    inserter.add_row(row)
                inserter.execute()

            logger.info('Added data to hyper file - ', datetime.datetime.now())

        logger.info("The connection to the Hyper file is closed. - ", datetime.datetime.now())


def appendHyperToDataSource(server, tableau_auth, old_datasource, NAME_TO_HYPER):
    with server.auth.sign_in(tableau_auth):
        logger.info('[Logged in successfully to {}]'.format(tableau_server_url), ' - ', datetime.datetime.now())
        # Upload Hyper file as data source, Mode = CreateNew, Overwrite, Append
        response = server.datasources.publish(old_datasource, NAME_TO_HYPER, 'Append')
        logger.info('Appended data to [', response.project_name, response.datasource_type, '] datasource',
                    datetime.datetime.now())

    return response.project_name, response.datasource_type, datetime.datetime.now()


def DeleteDataFromDS(server, tableau_auth, ds_id):
    logger.info(f'Delete data where VendorSettledDate greater than {three_month_ago} from - ', server.server_address)

    # delete last 3 month data from datasource
    json_request = {
        "actions": [
            {
                "action": "delete",
                "target-schema": "Extract",
                "target-table": "Extract",
                "condition": {"op": "or",
                              "args":
                                  [
                                      {
                                          "op": "gt",
                                          "target-col": "VendorSettledDate",
                                          "const": {"type": "string", "v": f"{three_month_ago}"}
                                      }
                                  ]
                              }
            }
        ]
    }

    with server.auth.sign_in(tableau_auth):
        header = {

            'X-Tableau-Auth': server.auth_token,
            'content-type': 'application/json',
            'accept': 'application/xml',
            'RequestID': str(uuid.uuid4()),

        }
        up_endpt = f'{server.server_address}/api/3.12/sites/{server.site_id}/datasources/{ds_id}/data'
        server_response = requests.patch(up_endpt, data=json.dumps(json_request), headers=header)
        logger.info(server_response.status_code, server_response.text)

if __name__ == "__main__":
    DW_df = DataFromDW()
    createHyperFromDF(New_Hyper_name, DW_df)
    DeleteDataFromDS(tb_server, tb_tableau_auth, old_datasource_id)
    appendHyperToDataSource(tb_server, tb_tableau_auth, old_datasource_item, New_Hyper_name)
