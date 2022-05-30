from dotenv import load_dotenv
import os
import logging

import tableauserverclient as TSC

load_dotenv()

logging.basicConfig(filename="tabData.log",
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

tableau_server_url = os.getenv('tableau_server_url')
tableau_token_name = os.getenv('tableau_token_name')
tableau_token_secret = os.getenv('tableau_token_secret')
tableau_site = os.getenv('tableau_site')

server = TSC.Server(tableau_server_url, use_server_version=True)
tableau_auth = TSC.PersonalAccessTokenAuth(token_name=tableau_token_name
                                           , personal_access_token=tableau_token_secret
                                           , site_id=tableau_site)

with server.auth.sign_in(tableau_auth):
    all_proj, pagination_item_p = server.projects.get()
    all_datasources, pagination_item = server.datasources.get()
    logger.info("\nThere are {} projects on site: ".format(pagination_item_p.total_available))
    logger.info([[datasource.name, datasource.id] for datasource in all_proj])
    logger.info("\nThere are {} datasiurces on site: ".format(pagination_item.total_available))
    logger.info([[datasource.name, datasource.id] for datasource in all_datasources])

