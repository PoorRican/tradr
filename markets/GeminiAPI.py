import base64
import datetime
import hashlib
import hmac
import json
import time
from os import path
from typing import Union

import pandas as pd
import requests

from models.data import json_to_df, DATA_ROOT
from markets.Market import Market
from models.trades import Trade, SuccessfulTrade

BASE_URL = "https://api.gemini.com"


class GeminiAPI(Market):
    name = 'Gemini'
    valid_freqs = ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day')

    def __init__(self, api_key, api_secret, time_frame='1m', root=DATA_ROOT):
        """
        Args:
            api_key: Gemini API key
            api_secret: Gemini API secret
            time_frame: ticker frequency
            root: root directory to store ticker data
        """
        super().__init__()
        assert time_frame in self.valid_freqs
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.freq = time_frame
        self.root = root

        self.load()
        self.update()
        self.save()

    def calc_fee(self) -> float:
        endpoint = '/v1/notionalvolume'
        response = self.post(endpoint)
        # because `place_order` uses fill-or-kill orders, order is subject to "taker" fee
        # see https://www.gemini.com/fees/activetrader-fee-schedule#section-overview
        try:
            return response['api_taker_fee_bps'] / 100
        except KeyError:
            print("Error in `MarketAPI.calc_fee`")
            print(response)
            return 0.35

    def get_candles(self) -> pd.DataFrame:
        """ Get all candles from Gemini (1m, 5m, 15m, 30m, 1hr)"""
        assert self.freq in self.valid_freqs

        response = requests.get(BASE_URL + "/v2/candles/btcusd/" + self.freq)
        btc_candle_data = response.json()
        data = json_to_df(btc_candle_data)

        # the Gemini API sends data in a reverse direction for some reason (lower index are later times)
        # TODO: check that GeminiAPI has correct order of data in unittests
        data = data.iloc[::-1]
        assert data.iloc[0].name < data.iloc[-1].name

        # set flag/metadata on `DataFrame`
        data.attrs['freq'] = self.freq
        return data

    @property
    def filename(self):
        """ Return """
        return path.join(self.root, self.name + '_' + self.freq + ".pkl")

    @staticmethod
    def get_orderbook() -> dict:
        response = requests.get(BASE_URL + "/v1/book/btcusd")
        return response.json()

    def update(self) -> None:
        """ Updates `data` with recent candle data """
        # self.data = combine_data(self.data, self.get_candles())
        self.data = self.get_candles()

    def load(self):
        try:
            self.data = pd.read_pickle(self.filename)
        except FileNotFoundError:
            pass

    def save(self):
        self.data.to_pickle(self.filename)

    def post(self, endpoint: str, data=None) -> dict:
        if data is None:
            data = dict()
        t = datetime.datetime.now()
        payload_nonce = time.time()

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

    def _convert(self, response: dict, trade: Trade) -> 'SuccessfulTrade':
        """ Translate exchange response into `SuccessfulTrade`.

        Used to store data from exchange. This is necessary because exchange data (such as "rate")
        might be different from the original data sent to the server and should therefore not be stored.
        """
        rate = response['price']
        amount = response['amt']
        _id = response['order_id']
        return SuccessfulTrade(amount, rate, trade.side, id=_id)

    def place_order(self, trade: Trade) -> Union['SuccessfulTrade', 'False']:
        """ Places an order - specifically a Fill-or-Kill Limit Order.

        As per Gemini documentation:
            Filled immediately at or better than a specified price. If the
            order cannot be filled in full immediately, the entire quantity
            is canceled. The order does not rest on the continuous order book.
        """
        data = {
            'symbol': "btcusd",
            'amount': trade.amt,
            'price': trade.rate,
            'side': trade.side,
            'type': "exchange limit",
            'options': ["fill-or-kill"]
        }

        response = self.post("/v1/order/new", data)
        if not response['is_cancelled']:  # order was fulfilled
            return self._convert(response, trade)
        else:
            return False

    def convert_freq(self, freq: str):
        """ Converts Gemini ticker interval to a value pandas can use.

        Examples:
            Gemini uses the string `15m` to denote an interval of 15 minutes, but
            pandas uses `15min` for the same.

        Args:
            freq: Gemini ticker interval

        Returns:

        """
        assert freq in self.valid_freqs

        if 'm' in freq:
            return freq + 'in'
        elif 'hr' in freq:
            return freq[:-2] + 'H'
        else:
            return '1D'



