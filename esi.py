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

def basic_request(url):
    request_headers = {
        "Accept-Encoding": "gzip",
        "Conent-Type": "application/json",
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


def fetch_kill(killID, hash):
    url = "https://esi.evetech.net/latest/killmails/{0}/{1}/?datasource=tranquility".format(killID, hash)
    return basic_request(url)


def fetch_player(characterID):
    url = "https://esi.evetech.net/latest/characters/{0}/?datasource=tranquility".format(characterID)
    return basic_request(url)