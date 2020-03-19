import logging
import requests
import re
import time

from config import logging_file

# Logging Configuration
logging.basicConfig(filename=logging_file, level=logging.DEBUG)
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


def fetch_report(url):
    request_headers = {
        "Accept-Encoding": "gzip",
        "User-Agent": "Biwako Acami Scrapper (biwakoacami@gmail.com)"
    }
    logger.debug("Requesting {0}".format(url))
    attempt = True
    retry = 3

    while attempt == True:
        try:
            logger.debug("Waiting 5 seconds before query")
            time.sleep(5)
            request = requests.get(url, headers=request_headers, timeout=30)
            attempt = False
        except requests.exceptions.Timeout as errt:
            logger.error("Timeout Error: {0}".format(errt))
            
            retry = retry - 1

            if retry == 0:
                return
            else:
                logger.info("Retrying {0} more times.".format(retry))

    data = request.json()

    # Break if the page is empty
    if len(data) == 0:
        logger.warn("Received an empty page")
    
    return data

def get_killreport(killID):
    # Constants
    url = "https://zkillboard.com/api/killID/{0}/".format(killID)

    return fetch_report(url)

def get_killreports(identifier, base):
    if base == 'character':
        modifier = 'characterID'
    elif base == 'corporation':
        modifier = 'corporationID'
    elif base == 'alliance':
        modifier = 'allianceID'
    elif base == 'ship':
        modifier = 'shipTypeID'
    elif base == 'system':
        modifier = 'solarSystemID'
    elif base == 'region':
        modifier = 'regionID'
    else:
        return

    uri = "https://zkillboard.com/api/{0}/{1}/page/{2}/"

    dump = []

    for page in range(1, 11):
        url = uri.format(modifier, identifier, page)
        data = fetch_report(url)
        
        # Break if the page is empty
        if len(data) == 0:
            break
        
        for report in data:
            dump.append(report)

    return sorted(dump, key = lambda report: report['killmail_id'], reverse = True)

def get_killreports_query(query):

    if re.search("killid", query, re.IGNORECASE):
        paged = False
        return fetch_report(query)

    else:
        paged = True
        uri = query + "page/{0}/"

        dump = []

        for page in range(1, 11):
            url = uri.format(page)
            data = fetch_report(url)
            
            # Break if the page is empty
            if len(data) == 0:
                break
            
            for report in data:
                dump.append(report)

        return sorted(dump, key = lambda report: report['killmail_id'], reverse = True)