import keyring
import quiverquant
import datetime
import time

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

MAX_TRADES = 10 # The maximum number of trades to hold at once.
POSITION_LENGTH = 55 # Number of days to hold a trade.
FUND_SIZE = 1000 # Total size of the fund in USD.
USE_PAPER = True # Use paper trading, no real money used.

def load_apis():
    quiver_api_key = keyring.get_password('quiverquant', 'api_key')
    quiver = quiverquant.quiver(quiver_api_key)

    (api_key_type, secret_key_type) = ('api_key_paper', 'secret_key_paper') if USE_PAPER else ('api_key', 'secret_key')
    api_key = keyring.get_password('alpaca', api_key_type)
    secret_key = keyring.get_password('alpaca', secret_key_type)

    if not api_key or not secret_key:
        raise Exception('Alpaca API key not found.')
    alpaca = TradingClient(api_key, secret_key, paper=USE_PAPER)

    return quiver, alpaca


def load_orders_df():
    df = quiver.senate_trading()
    
    # Filter out trades that were not stock purchases made within the position length.
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=POSITION_LENGTH)
    df = df[(df['Date'] >= cutoff_date) & (df['Transaction'] == 'Purchase')]
    
    # Weighting stocks to buy based on the aggregate of the dollar amount purchased.
    df.loc[:, 'Amount'] = df['Amount'] / df['Amount'].sum() * FUND_SIZE

    df = df.drop(columns=['Range', 'Senator', 'Transaction', 'Date'])
    df.reset_index(drop=True, inplace=True)

    return df


def init_buy_orders(orders_df):
    for _, order in orders_df.iterrows():
        ticker = order['Ticker']
        amount = round(order['Amount'], 2)

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
    ''' Wait a week until the next market open. '''

    week_open = clock.next_open + datetime.timedelta(days=6)
    print(f'Sleeping until {week_open}...')
    time_diff = (week_open - clock.timestamp).total_seconds()
    time.sleep(time_diff)

    while not clock.is_open:
        print(f'Market not open today. Sleeping until {clock.next_open}...')
        time_diff = (clock.next_open - clock.timestamp).total_seconds()
        time.sleep(time_diff)

    print('Market now open. Awakening...')

if __name__ == '__main__':
    quiver, alpaca = load_apis()
    clock = alpaca.get_clock()
    while True:
        orders_df = load_orders_df()
        if clock.is_open:
            sell_all()
            init_buy_orders(orders_df)
        fund_details()
        wait_for_market()