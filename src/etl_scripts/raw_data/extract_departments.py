"""
Extracts departments and Municipality/City from datos.gov.co API for Colombia.
"""
import logging
import sys
from typing import Generator

import requests

# Logger Configuration
logging.basicConfig(
    filename='./filelog.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger()


def extract_data(
        base_url: str,
        endpoint: str,
        limit: int = 1000,
        offset: int = 0
        ) -> Generator[dict, None, None]:
    """
    Extracts data from an API based on the provided base URL and endpoint.

    :param base_url: Base URL of the API.
    :param endpoint: Endpoint of the API to fetch data from.
    :param limit: Number of records to fetch per request (default is 100)/
    :param offset:  Offset value per pagination (default is 0)

    :returns Generator   Data items retrieved from the API
    """
    while True:
        params = {'$limit': limit, '$offset': offset}
        try:
            response = requests.get(base_url+endpoint,
                                    params=params,
                                    timeout=20)
            data = response.json()
            offset += limit
            if not data:
                break
            yield from data
        except requests.ConnectionError as ce:
            logging.error("An error ocurred while making the request. %s ", ce)
            sys.exit(1)


if __name__ == '__main__':
    # Configuration of the URL and data file
    URL = 'https://www.datos.gov.co/resource/'
    FILE = '95qx-tzs7.json'
    # Storage for extracted data
    stored_data = []
    # Generator to extract and sotre data
    data_generator = extract_data(base_url=URL, endpoint=FILE)
    for data_item in data_generator:
        stored_data.append(data_item)
