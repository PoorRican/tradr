from abc import abstractmethod, ABC
from os import path
from typing import Union

import base64
import datetime
import hashlib
import hmac
import json
import pandas as pd
import requests
import time

from data import json_to_df, combine_data, DATA_ROOT

BASE_URL = "https://api.gemini.com"


class Market(ABC):
    """ Core infrastructure which abstracts communication with exchange.

    Posts sell and buy orders, records historical ticker data.

    Todo:
        - Add a layer of risk management:
            - "runaway" trade decisions or unfavorable outcomes

    Attributes:
        data: available ticker data
    """
    def __init__(self):
        self.data = pd.DataFrame(columns=('open', 'high', 'low', 'close', 'volume'))

    @abstractmethod
    def update(self):
        """ Update ticker data """
        return NotImplemented

    def load(self):
        """ Load pickled data """
        return NotImplemented

    def save(self):
        """ Save pickled data """
        return NotImplemented

    @abstractmethod
    def place_order(self, amount: float, rate: float, side: str) -> Union[dict, bool]:
        """ Post order to market.
        Args:
            amount: amount of asset to trade
            rate: exchange rate between asset and fiat
            side: type of trade ('buy'/'sell')

        Returns:
            If the market accepted trade and the order was executed, details of trade are returned. This is
            necessary because the `rate` of trade might be better than requested.
        """
        return NotImplemented

    @abstractmethod
    def calc_fee(self, *args, **kwargs):
        """ Calculate cost of a transaction

        Returns:

        """
        return NotImplemented

    @property
    @abstractmethod
    def filename(self) -> str:
        return NotImplemented


class GeminiAPI(Market):
    def __init__(self, api_key, api_secret, time_frame='1m'):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.time_frame = time_frame

        self.load()
        self.update()
        self.save()

    def calc_fee(self) -> float:
        endpoint = '/v1/notionalvolume'
        response = self.post(endpoint)
        # because the order is not on the books, order is subject to "taker" fee
        # see https://www.gemini.com/fees/activetrader-fee-schedule#section-overview
        try:
            return response['api_taker_fee_bps'] / 100
        except KeyError:
            print("Error in `MarketAPI.calc_fee`")
            print(response)
            return 0.35

    def get_candles(self) -> dict:
        """ Get all candles from Gemini (1m, 5m, 15m, 30m, 1hr)"""
        if self.time_frame in ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day'):
            response = requests.get(BASE_URL + "/v2/candles/btcusd/" + self.time_frame)
            btc_candle_data = response.json()

            data = json_to_df(btc_candle_data)
            return data
        else:
            raise ValueError(self.time_frame + " is not valid.")

    @property
    def filename(self):
        """ Return """
        return path.join(DATA_ROOT, self.time_frame + ".pkl")

    @staticmethod
    def get_orderbook() -> dict:
        response = requests.get(BASE_URL + "/v1/book/btcusd")
        return response.json()

    def update(self) -> None:
        """ Updates `data` with recent candle data """
        self.data = combine_data(self.data, self.get_candles())

    def load(self):
        self.data = pd.read_pickle(self.filename)

    def save(self):
        self.data.to_pickle(self.filename)

    def post(self, endpoint: str, data=None) -> dict:
        if data is None:
            data = dict()
        t = datetime.datetime.now()
        payload_nonce = str(int(time.mktime(t.timetuple()) * 1001000 + t.microsecond))

        payload = {"request": endpoint, "nonce": payload_nonce}
        payload.update(data)

        encoded_payload = json.dumps(payload).encode()
        b64 = base64.b64encode(encoded_payload)
        sig = hmac.new(self.api_secret, b64, hashlib.sha384).hexdigest()

        headers = {
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "X-GEMINI-APIKEY": self.api_key,
            "X-GEMINI-PAYLOAD": b64,
            "X-GEMINI-SIGNATURE": sig,
            "Cache-Control": "no-cache"
        }

        response = requests.post(BASE_URL + endpoint, headers=headers, data=None)

        return response.json()

    def place_order(self, amount, rate, side) -> Union[dict, bool]:
        """ Places an order - specifically a Fill-or-Kill Limit Order.

        As per Gemini documentation:
            Filled immediately at or better than a specified price. If the
            order cannot be filled in full immediately, the entire quantity
            is canceled. The order does not rest on the continuous order book.
        """
        data = {
            'symbol': "btcusd",
            'amount': amount,
            'price': rate,
            'side': side,
            'type': "exchange limit",
            'options': ["fill-or-kill"]
        }

        response = self.post("/v1/order/new", data)
        if not response['is_cancelled']:  # order was fulfilled
            return response
        else:
            return False
