import keyring
import datetime
import time
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from senate_scraper import senate_trading

POSITION_LENGTH = 60 # Number of days to hold a trade.
FUND_SIZE = 1000 # Total size of the fund in USD.
USE_PAPER = True # Use paper trading, no real money used.

def load_alpaca():
    (api_key_type, secret_key_type) = ('api_key_paper', 'secret_key_paper') if USE_PAPER else ('api_key', 'secret_key')
    api_key = keyring.get_password('alpaca', api_key_type)
    secret_key = keyring.get_password('alpaca', secret_key_type)

    if not api_key or not secret_key:
        raise Exception('Alpaca API key not found.')
    alpaca = TradingClient(api_key, secret_key, paper=USE_PAPER)

    return alpaca


def load_orders() -> pd.DataFrame:
    df = senate_trading()
    # Filter out trades that were not stock purchases made within the position length.
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=POSITION_LENGTH)
    df = df[(df['tx_date'] >= cutoff_date) & (df['type'] == 'Purchase')]

    # Weighting stocks to buy based on the aggregate of the dollar amount purchased.
    df.loc[:, 'weighted_amount'] = df['tx_amount'] / df['tx_amount'].sum() * FUND_SIZE
    df.reset_index(drop=True, inplace=True)
    return df


def init_buys(orders_df):
    for _, order in orders_df.iterrows():
        ticker = order['ticker']
        amount = round(order['weighted_amount'], 2)
        if alpaca.get_asset(ticker).fractionable:
            print(f'Initiating buy: {ticker} - ${amount}.')

            order = alpaca.submit_order(
                order_data = MarketOrderRequest(
                    symbol=ticker,
                    notional=amount,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
            )
            print(f'Bought: {order.symbol} @ ${order.filled_avg_price}. Total ${order.notional}')
        else:
            print(f'Skipping {ticker}. Not fractionable.')
    print(f'All positions filled. Total exposure now: ${alpaca.get_account().long_market_value}')


def sell_all():
    print('Closing all positions.')
    alpaca.close_all_positions(cancel_orders=True)


def fund_details():
    account = alpaca.get_account()
    print(f'\n========== Fund Details ==========')
    print(f'Fund Equity: ${float(account.equity):.2f}')
    print(f'Last Equity: ${float(account.last_equity):.2f}')
    print(f'Cash: ${float(account.cash):.2f}')
    print(f'Fees: ${float(account.accrued_fees):.2f}')
    print(f'Currency: {account.currency}')
    print(f'Date: {datetime.datetime.now()}')
    print(f'==================================\n')


def wait_for_market():
    ''' Wait until the next market open. '''
    if clock.timestamp < latest_rebalance + datetime.timedelta(days=7):
        if not clock.is_open:
            print(f'Market is closed. Sleeping until {clock.next_open}...')
            t_delta = (clock.next_open - clock.timestamp).total_seconds()
            time.sleep(t_delta)
            print('Market now open. Awakening...')

def rebalance():
    ''' Sells all assets then creates new orders. '''
    sell_all()
    orders = load_orders()
    init_buys(orders)
    return clock.timestamp

if __name__ == '__main__':
    alpaca = load_alpaca()
    clock = alpaca.get_clock()
    latest_rebalance = rebalance()
    while True:
        wait_for_market()
        latest_rebalance = rebalance()
        fund_details()