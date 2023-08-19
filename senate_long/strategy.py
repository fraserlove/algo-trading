import keyring
import datetime
import time
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from scraper import SenateScraper

class Strategy:

    def __init__(self, use_paper: bool = True, position_length: int = 60, rebalance_frequency: int = 7):
        self.use_paper = use_paper # Use paper trading, no real money used.
        self.position_length = position_length # Number of days to hold a trade.
        self.rebalance_frequency = rebalance_frequency # Number of days to wait between rebalancing.

    
    def run(self):
        ''' Runs the strategy, performing an initial rebalance and then rebalancing accordingly. '''

        self.trade_client = self._load_client()
        self.account = self.trade_client.get_account()
        self.clock = self.trade_client.get_clock()
        # Perform initial balancing of portfolio now.
        self.next_rebalance = self.clock.timestamp

        self.fund_details()
        while True:
            # If a rebalance is due, perform one and display the updated fund details.
            if self.clock.timestamp >= self.next_rebalance:
                self.rebalance()
                self.fund_details()
            # Wait for the next market open.
            self.wait_for_market()


    def _load_client(self) -> TradingClient:
        '''
        Load the Alpaca trading client.

        :return: An instance of the Alpaca TradingClient.
        '''

        # Fetch Alpaca API keys from Apple keychain.
        (api_key_type, secret_key_type) = ('api_key_paper', 'secret_key_paper') if self.use_paper else ('api_key', 'secret_key')
        api_key = keyring.get_password('alpaca', api_key_type)
        secret_key = keyring.get_password('alpaca', secret_key_type)

        if not api_key or not secret_key:
            raise Exception('Alpaca API key not found.')
        
        if not self.use_paper:
            print('WARNING: Live Trading.')

        return TradingClient(api_key, secret_key, paper=self.use_paper)


    def load_orders(self) -> pd.DataFrame:
        '''
        Load and process trading orders.
        
        :return: A DataFrame containing trading orders.
        '''

        orders = SenateScraper().senate_trading(lookback_period=self.position_length, tx_type='Purchase')
        # Weighting stocks to buy based on the aggregate of the dollar amount purchased.
        cash = float(self.trade_client.get_account().cash)
        orders.loc[:, 'weighted_amount'] = orders['tx_amount'] / orders['tx_amount'].sum() * cash
        return orders


    def buy_orders(self, orders: pd.DataFrame) -> None:
        '''
        Initialise buy orders based on the provided DataFrame of orders.
        
        :param orders_df: A DataFrame containing trading orders.
        '''

        print(f'INFO: {self.clock.timestamp}: Initiating {len(orders)} buy orders...')
        for _, order in orders.iterrows():
            ticker = order['ticker']

            # Check asset is fractionable through Alpaca.
            if self.trade_client.get_asset(ticker).fractionable:
                # Round amount to comply with Alpaca fractional trading limits.
                amount = round(order['weighted_amount'], 2)
                order = self.trade_client.submit_order(
                    order_data = MarketOrderRequest(
                        symbol=ticker,
                        notional=amount,
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY
                    )
                )
                print(f'BUY: ${order.notional} of {order.symbol}')
            else:
                print(f'INFO: Skipping {ticker}. Not fractionable via Alpaca.')
        print(f'INFO: {self.clock.timestamp}: All {len(self.orders)} orders initiated. Total exposure now: ${float(self.account.long_market_value):.2f}')


    def close_all(self) -> None:
        ''' Sell all positions and close all orders. '''

        print('INFO: Closing all positions.')
        self.trade_client.close_all_positions(cancel_orders=True)


    def fund_details(self) -> None:
        ''' Print details about the funds state. '''

        positions = self.trade_client.get_all_positions()

        print(f'\n========== Fund Details ==========')
        print(f'Current Time: {self.clock.timestamp}')
        print(f'Fund Equity: ${float(self.account.equity):.2f}')
        print(f'Last Equity: ${float(self.account.last_equity):.2f}')
        print(f'Cash: ${float(self.account.cash):.2f}')
        print(f'Fees: ${float(self.account.accrued_fees):.2f}')
        print(f'Open Positions: {len(positions)}')
        print(f'Currency: {self.account.currency}')
        print(f'Next Rebalance: {self.next_rebalance}')
        print(f'==================================\n')


    def wait_for_market(self) -> None:
        ''' Wait until the next market open. '''

        print(f'INFO: {self.clock.timestamp}: Waiting for next market open. Sleeping until {self.clock.next_open}...')
        try:
            time.sleep((self.clock.next_open - self.clock.timestamp).total_seconds())
        except KeyboardInterrupt:
            print('Exiting strategy...')
            return
        print(f'INFO: {self.clock.timestamp}: Market now open. Awakening...')


    def rebalance(self):
        ''' Perform the rebalancing process by selling all assets and creating new orders. '''

        print(f'INFO: {self.clock.timestamp}: Initiating Rebalance...')

        self.close_all()
        orders = self.load_orders()
        self.buy_orders(orders)
        try:
            self.close_all()
            orders = self.load_orders()
            self.buy_orders(orders)
            # Update the time until the next rebalance is due.
            self.next_rebalance += datetime.timedelta(days=self.rebalance_frequency)
        except:
            print('ERROR: Rebalance failed. Trying again at next market open.')
            self.next_rebalance = self.clock.next_open
    

if __name__ == '__main__':
    senate_long = Strategy()
    senate_long.run()