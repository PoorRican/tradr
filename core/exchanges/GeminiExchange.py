from typing import Union

from core.proto.gemini import GeminiProto
from core.exchange import Exchange
from models.trades import Trade, SuccessfulTrade


class GeminiExchange(GeminiProto, Exchange):
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

