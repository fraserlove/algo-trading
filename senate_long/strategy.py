import keyring
import datetime
import time
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from scraper import senate_trading

USE_PAPER = True # Use paper trading, no real money used.
POSITION_LENGTH = 60 # Number of days to hold a trade.
REBALANCE_FREQUENCY = 7 # Number of days to wait between rebalancing.

def load_alpaca() -> TradingClient:
    '''
    Load the Alpaca trading client.
    
    :return: An instance of the Alpaca TradingClient.
    '''

    # Fetch Alpaca API keys from Apple keychain.
    (api_key_type, secret_key_type) = ('api_key_paper', 'secret_key_paper') if USE_PAPER else ('api_key', 'secret_key')
    api_key = keyring.get_password('alpaca', api_key_type)
    secret_key = keyring.get_password('alpaca', secret_key_type)

    if not api_key or not secret_key:
        raise Exception('Alpaca API key not found.')
    if not USE_PAPER:
        print('WARNING: Live Trading.')
    return TradingClient(api_key, secret_key, paper=USE_PAPER)


def load_orders() -> pd.DataFrame:
    '''
    Load and process trading orders.

    :return: A DataFrame containing trading orders.
    '''

    df = senate_trading()
    # Filter out trades that were not stock purchases made within the position length.
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=POSITION_LENGTH)
    df = df[(df['tx_date'] >= cutoff_date) & (df['type'] == 'Purchase')]

    # Weighting stocks to buy based on the aggregate of the dollar amount purchased.
    cash = float(alpaca.get_account().cash)
    df.loc[:, 'weighted_amount'] = df['tx_amount'] / df['tx_amount'].sum() * cash
    df.reset_index(drop=True, inplace=True)
    return df


def init_buys(orders_df: pd.DataFrame) -> None:
    '''
    Initialise buy orders based on the provided DataFrame of orders.

    :param orders_df: A DataFrame containing trading orders.
    '''

    print(f'INFO: {clock.timestamp}: Initiating {len(orders_df)} buy orders...')
    for _, order in orders_df.iterrows():
        ticker = order['ticker']

        # Round amount to comply with Alpaca fractional trading limits.
        amount = round(order['weighted_amount'], 2)
        if not alpaca.get_asset(ticker).fractionable:
            continue # TODO: Make orders on non-fractionable trades.

        # Check asset is tradable through Alpaca.
        if alpaca.get_asset(ticker).tradable:
            order = alpaca.submit_order(
                order_data = MarketOrderRequest(
                    symbol=ticker,
                    notional=amount,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
            )
            print(f'BUY: ${order.notional} of {order.symbol}')
        else:
            print(f'INFO: Skipping {ticker}. Not tradable via Alpaca.')
    print(f'INFO: {clock.timestamp}: All {len(orders_df)} orders initiated. Total exposure now: ${float(alpaca.get_account().long_market_value):.2f}')


def sell_all() -> None:
    ''' Sell all positions and close all orders. '''

    print('INFO: Closing all positions.')
    alpaca.close_all_positions(cancel_orders=True)


def fund_details() -> None:
    ''' Print details about the funds state. '''

    account = alpaca.get_account()
    positions = alpaca.get_all_positions()

    print(f'\n========== Fund Details ==========')
    print(f'Current Time: {clock.timestamp}')
    print(f'Fund Equity: ${float(account.equity):.2f}')
    print(f'Last Equity: ${float(account.last_equity):.2f}')
    print(f'Cash: ${float(account.cash):.2f}')
    print(f'Fees: ${float(account.accrued_fees):.2f}')
    print(f'Open Positions: {len(positions)}')
    print(f'Currency: {account.currency}')
    print(f'Date: {datetime.datetime.now()}')
    print(f'Next Rebalance: {next_rebalance}')
    print(f'==================================\n')


def wait_for_market() -> None:
    ''' Wait until the next market open. '''

    print(f'INFO: {clock.timestamp}: Waiting for next market open. Sleeping until {clock.next_open}...')
    t_delta = (clock.next_open - clock.timestamp).total_seconds()
    time.sleep(t_delta)
    print(f'INFO: {clock.timestamp}: Market now open. Awakening...')


def rebalance() -> datetime.datetime:
    '''
    Perform the rebalancing process by selling all assets and creating new orders.

    :param datetime.datetime: The datetime of the next rebalancing.
    '''

    print(f'INFO: {clock.timestamp}: Initiating Rebalance...')
    try:
        orders = load_orders()
        sell_all()
        init_buys(orders)
        # Calculate the time until the next rebalance is due.
        if clock.is_open:
            return clock.next_open + datetime.timedelta(days=REBALANCE_FREQUENCY - 1)
        return clock.next_open + datetime.timedelta(days=REBALANCE_FREQUENCY)
    except:
        print('ERROR: Rebalance failed. Trying again at next market open.')
        # If rebalance fails, try again a the next market open.
        return clock.next_open
    

if __name__ == '__main__':
    alpaca = load_alpaca()
    clock = alpaca.get_clock()
    fund_details()
    # Perform initial balancing of portfolio at the next market open.
    next_rebalance = clock.timestamp

    while True:
        # If a rebalance is due, perform one and display the updated fund details.
        if clock.timestamp >= next_rebalance:
            next_rebalance = rebalance()
            fund_details()
        # Wait for the next market open.
        wait_for_market()