""" Scrape the stock transactions from Senator periodic filings. """

import bs4
import pandas as pd
import datetime
import requests

ROOT = 'https://efdsearch.senate.gov'
LANDING_PAGE_URL = f'{ROOT}/search/home/'
SEARCH_PAGE_URL = f'{ROOT}/search/'
REPORTS_URL = f'{ROOT}/search/report/data/'
PDF_PREFIX = '/search/view/paper/'

BATCH_SIZE = 100 # Batch size when fetching reports. Must be a max of 100.
LOOKBACK_PERIOD = 500 # Window (in days) to search for senate trades.

HEADER = ['senator', 'tx_date', 'file_date', 'ticker', 'type', 'tx_amount']

def _csrf(client: requests.Session) -> str:
    """ Set the session ID and return the CSRF token for this session. """
    landing_page = bs4.BeautifulSoup(client.get(LANDING_PAGE_URL).text, 'lxml')

    form_csrf = landing_page.find(attrs={'name': 'csrfmiddlewaretoken'})['value']
    form_payload = { 'csrfmiddlewaretoken': form_csrf, 'prohibition_agreement': '1' }
    client.post(LANDING_PAGE_URL, data=form_payload, headers={'Referer': LANDING_PAGE_URL})

    return client.cookies['csrftoken'] if 'csrftoken' in client.cookies else client.cookies['csrf']


def senator_reports(client: requests.Session) -> list[list[str]]:
    """ Return all results from the periodic transaction reports API. """
    token = _csrf(client)
    idx = 0
    reports = reports_api(client, idx, token)
    all_reports: list[list[str]] = []
    while len(reports) != 0:
        all_reports.extend(reports)
        idx += BATCH_SIZE
        reports = reports_api(client, idx, token)
    return all_reports


def reports_api(client: requests.Session, offset: int, token: str) -> list[list[str]]:
    """ Query the periodic transaction reports API. """
     # Only request reports that were made within the position maximum fetch window.
    cutoff = (datetime.datetime.now() - datetime.timedelta(days=LOOKBACK_PERIOD)).strftime('%m/%d/%Y')
    login_data = {
        'start': str(offset),
        'length': str(BATCH_SIZE),
        'report_types': '[11]',
        'submitted_start_date': f'{cutoff} 00:00:00',
        'csrfmiddlewaretoken': token
    }
    # print(f'Getting rows starting at {offset}')
    response = client.post(REPORTS_URL, data=login_data, headers={'Referer': SEARCH_PAGE_URL})
    if not response.status_code == 200:
        print('Failed to fetch senate trading data:', response.status_code)
    return response.json()['data']


def _tbody_from_link(client: requests.Session, link: str) -> bs4.element.Tag | None:
    """
    Return the tbody element containing transactions for this senator.
    Return None if no such tbody element exists.
    """
    report_url = '{0}{1}'.format(ROOT, link)
    report_response = client.get(report_url)
    # If the page is redirected, then the session ID has expired
    if report_response.url == LANDING_PAGE_URL:
        print('Resetting CSRF token and session cookie')
        _csrf(client)
        report_response = client.get(report_url)
    report = bs4.BeautifulSoup(report_response.text, 'lxml')
    tbodies = report.find_all('tbody')
    if len(tbodies) == 0:
        return None
    return tbodies[0]


def txs_for_report(client: requests.Session, row: list[str]) -> pd.DataFrame:
    """
    Convert a row from the periodic transaction reports API to a DataFrame
    of transactions.
    """
    first, last, _, link_html, file_date = row
    link = bs4.BeautifulSoup(link_html, 'lxml').a.get('href')
    tbody = _tbody_from_link(client, link)

    # Cannot parse PDFs or empty webpages.
    if link.startswith(PDF_PREFIX) or not tbody:
        return pd.DataFrame()

    stocks = []
    for table_row in tbody.find_all('tr'):
        cols = [c.get_text() for c in table_row.find_all('td')]
        tx_amount = int(cols[7].split('$')[-1].replace(',', ''))
        tx_date = cols[1].strip()
        ticker = cols[3].strip()
        order_type = cols[6].strip()

        if ticker.strip() not in ('--', ''):
            stocks.append([
                f'{first} {last}',
                tx_date,
                file_date,
                ticker,
                order_type,
                tx_amount
            ])

    return pd.DataFrame(stocks).rename(columns=dict(enumerate(HEADER)))


def senate_trading() -> pd.DataFrame:
    print('Searching for senate trades...')
    client = requests.Session()
    reports = senator_reports(client)
    txs_total = pd.DataFrame()
    for i, row in enumerate(reports):
        txs = txs_for_report(client, row)
        if not txs.empty: # The transactions DataFrame is empty if the report is not in tabular format.
            txs_total = pd.concat([txs_total, pd.DataFrame(txs)], ignore_index=True)
            txs_total['tx_date'] = pd.to_datetime(txs_total['tx_date'])
            txs_total['file_date'] = pd.to_datetime(txs_total['file_date'])
            # print(f'Fetched report {i} from {pd.DataFrame(txs)["file_date"][0]}.')
    txs_total = txs_total.drop(columns=['file_date'])
    print(f'Found {len(txs_total)} total trades.')
    return txs_total.sort_values('tx_date', ascending=False)