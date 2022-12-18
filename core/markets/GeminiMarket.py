import base64
import hashlib
import hmac
import json
import time
from typing import Union, Optional
import pandas as pd
import requests
import logging

from core.MarketAPI import MarketAPI
from core.misc import TZ
from models import json_to_df, Trade, SuccessfulTrade


class GeminiMarket(MarketAPI):
    """ Primary interface for interacting with the Gemini market/exchange.

    Contains methods and attributes that retrieves candle data, fetches current orderbook, and posts trades.

    Fields:
        __name__ (str):
            Platform name. Used for setting flag attributes and filenames.

        valid_freqs (tuple[str, ...]):
            iterable with valid frequency/interval values. This will be changed into an `enum` in the near
            future.

        asset_pairs (tuple[str, ...]):
            List of valid asset pairs. This list is pulled from documentation.
            https://docs.gemini.com/rest-api/#symbols-and-minimums

        BASE_URL (str):
            Base URL for accessing all API endpoints. This should be set to `'api.sandbox.gemini.com'`
            during testing or simulation.
    """
    __name__: str = 'Gemini'
    valid_freqs: tuple[str, ...] = ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day')
    # noinspection SpellCheckingInspection
    asset_pairs = set("btcusd ethbtc ethusd zecusd zecbtc zeceth zecbch zecltc bchusd bchbtc bcheth "
                      "ltcusd ltcbtc ltceth ltcbch batusd daiusd linkusd oxtusd batbtc linkbtc oxtbtc "
                      "bateth linketh oxteth ampusd compusd paxgusd mkrusd zrxusd kncusd manausd storjusd "
                      "snxusd crvusd balusd uniusd renusd umausd yfiusd btcdai ethdai aaveusd filusd btceur "
                      "btcgbp etheur ethgbp btcsgd ethsgd sklusd grtusd bntusd 1inchusd enjusd lrcusd sandusd "
                      "cubeusd lptusd bondusd maticusd injusd sushiusd dogeusd alcxusd mirusd ftmusd ankrusd "
                      "btcgusd ethgusd ctxusd xtzusd axsusd slpusd lunausd ustusd mco2usd efilfil gusdusd "
                      "dogebtc dogeeth wcfgusd rareusd radusd qntusd nmrusd maskusd fetusd ashusd audiousd "
                      "api3usd usdcusd shibusd rndrusd mcusd galausd ensusd kp3rusd cvcusd elonusd mimusd "
                      "spellusd tokeusd ldousd rlyusd solusd rayusd sbrusd apeusd rbnusd fxsusd dpiusd "
                      "lqtyusd lusdusd fraxusd indexusd mplusd gusdsgd metisusd qrdousd zbcusd chzusd "
                      "revvusd jamusd fidausd gmtusd orcausd gfiusd aliusd truusd gusdgbp dotusd ernusd "
                      "galusd eulusd samousd bicousd imxusd plausd iotxusd busdusd avaxusd".split(' '))

    BASE_URL: str = "https://api.gemini.com"
    _SECRET_FN = "gemini_api.yml"
    _INSTANCES_FN = "gemini_instances.yml"

    def get_fee(self) -> float:
        """ Retrieve current transaction fee.

        Notes:
            With the current implementation, all trades are subject to the "taker fee", because
            `place_order()` places "fill-or-kill" orders and thereby decreasing liquidity. In the future,
            other order types will be implemented and the higher fee reduced.

            TODO:
                - How is this calculated?? Is this fee calculated per unit of asset traded? Is this a percentage?

        References:
            **API docs for retrieving fee**: https://docs.gemini.com/rest-api/#get-notional-volume

            **Gemini fee documentation**: https://www.gemini.com/fees/activetrader-fee-schedule#section-overview

        Warnings:
            Warning is thrown when there is an error with endpoint response leading default value is returned.

        Returns:
            Fee as returned by API. By default, `0.35` is returned.

        """
        endpoint = '/v1/notionalvolume'     # noqa
        response = self._post(endpoint)
        try:
            return response['api_taker_fee_bps'] / 100
        except KeyError:
            logging.warning("`MarketAPI.calc_fee` has no value for `fee`")
            return 0.35

    def _fetch_candles(self, freq: Optional[str] = None) -> pd.DataFrame:
        """ Low-level function to retrieve candle data.

        Ticker frequency is determined by `self.freq` and notated on return type via the `DataFrame.attrs` convention.
        This metadata can be referenced later by the qualitative infrastructure.

        Notes:
            The Gemini API sends time-series data backwards (last index is oldest record, instead of earliest).

            TODO:
                - Get all candle data and store in dict-like object

        References:
            https://docs.gemini.com/rest-api/#candles

        Args:
            freq:
                Optional value denoting desired candle frequency.
                When false-y, `self.freq` is used to fetch candle data.

        Raises:
            AssertionError: if `freq` is not a valid interval.

        Returns:
            Ticker data translated to `DataFrame` time-series.

        """
        assert freq in self.valid_freqs

        response = requests.get(self.BASE_URL + f"/v2/candles/{self.symbol}/{freq}")
        raw_candle_data = response.json()
        data = json_to_df(raw_candle_data)

        # reverse so data is ascending (oldest to most recent)
        data = data.iloc[::-1]
        assert data.iloc[0].name < data.iloc[-1].name

        # set flag/metadata on `DataFrame`
        # TODO: use pandas' built-in `freq` value for index
        data.attrs['freq'] = freq
        data.index = data.index.tz_localize(TZ, ambiguous='infer')

        return data

    def get_orderbook(self) -> dict:
        """ Fetch current orderbook.

        Orderbook is stored as 2 arrays containing mappings for each price.

        Notes:
            This is not currently implemented, but should be used in the future for calculating trade prices.

            The quantities and prices returned are returned as strings rather than numbers. The numbers returned are
            exact, not rounded, and it can be dangerous to treat them as floating point numbers.

            DO NOT USE THE TIMESTAMPS found in each price level. According to API docs: this field is included for
            compatibility reasons only and is just populated with a dummy value.

        References:
            https://docs.gemini.com/rest-api/#current-order-book

        Returns:
            `dict` with 2 keys ('bids'/'asks') mapping to arrays of bid price levels currently on the book. Each price
                level represents offers at that price. Each price level contains another mapping for [price, amount].
                See
        """
        response = requests.get(self.BASE_URL + f"/v1/book/{self.symbol}")
        return response.json()

    def _post(self, endpoint: str, data: dict = None) -> dict:
        """ Access private API endpoints.

        Handles payload encapsulation in accordance with API docs.

        References:
            https://docs.gemini.com/rest-api/#private-api-invocation
        """
        if data is None:
            data = dict()
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

        response = requests.post(self.BASE_URL + endpoint, headers=headers, data=None)

        return response.json()

    def _translate(self, response: dict, trade: Trade) -> 'SuccessfulTrade':
        """ Translate exchange response into `SuccessfulTrade`.

        Notes:
            Translation from exchange data is necessary because exchange data (such as "rate")
            might be different from the original data sent to the server and should therefore not be stored.

        References:
            Values returned from API: https://docs.gemini.com/rest-api/#order-status

        Args:
            response:
                Data returned from API endpoint.

            trade:
                Data sent to API endpoint.

        Returns:
            `SuccessfulTrade` containing data from API endpoint.
        """
        rate = response['price']
        amount = response['amt']
        _id = response['order_id']
        return SuccessfulTrade(amount, rate, trade.side, id=_id)

    def post_order(self, trade: Trade) -> Union['SuccessfulTrade', 'False']:
        """ Places an order - specifically a Fill-or-Kill Limit Order.

        Notes:
            As per Gemini documentation: "Filled immediately at or better than a specified price. If the order cannot be
            filled in full immediately, the entire quantity is canceled. The order does not rest on the continuous
            order book."

        References:
            https://docs.gemini.com/rest-api/#new-order

        Args:
            trade:
                Data to post to endpoint.

        Returns:
            `SuccessfulTrade`, when trade is accepted by market. Intended to be stored in `Strategy.orders` by calling
                function. `False`, When market rejects trade or otherwise fails. The calling function should then store
                the original data in a separate container (ie: `failed_orders`)
        """
        data = {
            'symbol': self.symbol,
            'amount': trade.amt,
            'price': trade.rate,
            'side': trade.side,
            'type': "exchange limit",
            'options': ["fill-or-kill"]
        }

        response = self._post("/v1/order/new", data)
        if not response['is_cancelled']:  # order was fulfilled
            return self._translate(response, trade)
        else:
            return False

    def translate_period(self, freq: str) -> str:
        """ Converts Gemini candle interval to a valid value for `pandas.DateOffset`.

        Notes:
            Used to as argument to `pandas.date_range()`, but should be valid as `pandas.Period`, `pandas.Timedelta`,
            or any other `DateOffset`.

        References:
            View Pandas documentation for a list of valid values:
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases

        Examples:
            Gemini uses the string `15m` to denote an interval of 15 minutes, but
            pandas uses `15min` for the same.

        Args:
            freq:
                Gemini candle interval.

        Raises:
            AssertionError: if `freq` is not a valid interval.

        Returns:
            Translated `DateOffset`.
        """
        assert freq in self.valid_freqs

        if 'm' in freq:
            return freq + 'in'
        elif 'hr' in freq:
            return freq[:-2] + 'H'
        else:
            return '1D'
