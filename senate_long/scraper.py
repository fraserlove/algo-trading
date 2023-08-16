import bs4
import pandas as pd
import datetime
import requests

# URLs for the Senate Electronic Financial Disclosure (EFD) search.
ROOT = 'https://efdsearch.senate.gov'
LANDING_URL = f'{ROOT}/search/home/'
SEARCH_URL = f'{ROOT}/search/'
REPORTS_URL = f'{ROOT}/search/report/data/'

BATCH_SIZE = 100 # Number of records to process in a single batch. Must be max of 100.
LOOKBACK_PERIOD = 100 # Lookback period (in days) to search for records.

# Header names for the columns in the generated dataframe.
HEADER = ['senator', 'tx_date', 'file_date', 'ticker', 'type', 'tx_amount']

def csrf(client: requests.Session) -> str:
    '''
    Set the session ID and return the CSRF token for this session.
    
    :param client: A `requests.Session` object representing the client's session.
    :return: The CSRF token extracted from the session cookies.
    '''

    # Fetch the landing page using the client session.
    landing_page = bs4.BeautifulSoup(client.get(LANDING_URL).text, 'lxml')
    # Extract the CSRF token from the HTML form on the landing page.
    form_csrf = landing_page.find(attrs={'name': 'csrfmiddlewaretoken'})['value']
    # Prepare the payload for the form submission including the CSRF token.
    form_payload = { 'csrfmiddlewaretoken': form_csrf, 'prohibition_agreement': '1' }
    # Submit the form using a POST request to set the session's CSRF token.
    client.post(LANDING_URL, data=form_payload, headers={'Referer': LANDING_URL})
    # Return the CSRF token from the session cookies.
    return client.cookies['csrftoken'] if 'csrftoken' in client.cookies else client.cookies['csrf']


def reports_api(client: requests.Session, offset: int, token: str) -> list[list[str]]:
    '''
    Query the periodic transaction reports API and return the fetched data.

    :param client: A `requests.Session` object representing the client's session.
    :param offset: The starting index of the reports to fetch.
    :param token: The CSRF token to include in the request.
    :return: A list of lists containing the fetched transaction report data.
    '''
    
    # Only request reports that were made within the lookback period.
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=LOOKBACK_PERIOD)).strftime('%m/%d/%Y')
    login_data = {
        'start': str(offset),
        'length': str(BATCH_SIZE),
        'report_types': '[11]',
        'submitted_start_date': f'{cutoff} 00:00:00',
        'csrfmiddlewaretoken': token
    }
    # Send a POST request to the reports API to fetch the data.
    response = client.post(REPORTS_URL, data=login_data, headers={'Referer': SEARCH_URL})
    if not response.status_code == 200:
        print(f'Failed to fetch senate trading data: {response.status_code}')
    # Extract and return the data from the response in JSON format.
    return response.json()['data']


def senator_reports(client: requests.Session) -> list[list[str]]:
    '''
    Fetch and return all results from the periodic transaction reports API.

    :param client: A `requests.Session` object representing the client's session.
    :return: A list of lists containing the transaction report data.
    '''

    i = 0
    all_reports = []
    token = csrf(client)
    # Fetch the initial batch of reports from the API.
    reports = reports_api(client, i, token)
    
    # Fetch reports from the API in batches until no more reports are received.
    while len(reports):
        # Extend the list of all_reports with the reports fetched in this batch.
        all_reports.extend(reports)
        # Move to the next batch.
        i += BATCH_SIZE
        # Fetch reports for the current batch.
        reports = reports_api(client, i, token)
    return all_reports


def fetch_tbody(client: requests.Session, link: str) -> bs4.element.Tag:
    '''
    Returns the tbody element containing transactions for this senator.

    :param client: A `requests.Session` object representing the client's session.
    :param link: The relative link to the senator's report.
    :return: A `bs4.element.Tag` representing the tbody element.
    '''

    # Construct the full URL for the senator's report using the provided link.
    report_url = f'{ROOT}{link}'
    # Fetch the response from the senator's report URL.
    report_response = client.get(report_url)
    # Parse the report page using BeautifulSoup with the 'lxml' parser.
    report = bs4.BeautifulSoup(report_response.text, 'lxml')
    # Return the first tbody instance, which contains the transaction data.
    return report.find('tbody')


def fetch_txs(client: requests.Session, row: list[str]) -> pd.DataFrame:
    '''
    Convert a row from the periodic transaction reports API to a DataFrame
    of transactions.

    :param client: A `requests.Session` object representing the client's session.
    :param row: A list representing a row of data from the periodic transaction reports API.
    :return: A DataFrame containing transaction data.
    '''

    # Extract relevant data from the row.
    first, last, _, link_html, file_date = row
    # Parse the link HTML to extract the report link.
    link = bs4.BeautifulSoup(link_html, 'lxml').a.get('href')
    # Fetch the tbody element containing transactions from the report.
    tbody = fetch_tbody(client, link)

    stocks = []
    # Check that tbody exists.
    if tbody:
        # Iterate through each table row within the tbody.
        for table_row in tbody.find_all('tr'):
            # Extract data from each column of the table row.
            cols = [c.get_text() for c in table_row.find_all('td')]

            # Extract relevant transaction details from the columns.
            tx_amount = int(cols[7].split('$')[-1].replace(',', ''))
            tx_date = cols[1].strip()
            ticker = cols[3].strip()
            order_type = cols[6].strip()

            # Append transaction details to the 'stocks' list.
            if ticker.strip() not in ('--', ''):
                stocks.append([f'{first} {last}', tx_date, file_date, ticker, order_type, tx_amount])
                
    # Create a DataFrame from the 'stocks' list and rename columns using HEADER.
    return pd.DataFrame(stocks).rename(columns=dict(enumerate(HEADER)))


def senate_trading() -> pd.DataFrame:
    '''
    Search for senate trades, fetch transaction data, and return as a DataFrame.

    :return: A DataFrame containing senate trading transaction data.
    '''

    print('Searching for senate trades...')
    client = requests.Session()
    # Fetch reports containing Senate trading data.
    reports = senator_reports(client)

    all_txs = pd.DataFrame()
    # Loop through each report and fetch transaction data.
    for report in reports:
        # Fetch transactions for the current report.
        txs = fetch_txs(client, report)
        # Check that transactions are present.
        if not txs.empty:
            # Concatenate the fetched transactions to the overall DataFrame.
            all_txs = pd.concat([all_txs, pd.DataFrame(txs)], ignore_index=True)

            # Convert 'tx_date' and 'file_date' columns to datetime objects.
            all_txs['tx_date'] = pd.to_datetime(all_txs['tx_date'])
            all_txs['file_date'] = pd.to_datetime(all_txs['file_date'])

    print(f'Found {len(all_txs)} trades.')
    # Sort transactions by 'tx_date' in descending order and return the DataFrame.
    return all_txs.sort_values('tx_date', ascending=False)