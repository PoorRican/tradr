from typing import Union

from markets.Market import Market


class SimulatedMarket(Market):
    """
    Mock class of `Market`.
    Has flat fee, always accepts an order.
    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    def update(self):
        pass

    def __init__(self):
        super().__init__()

    def place_order(self, amount, rate, side) -> Union[dict, bool]:
        return {'amount': amount,
                'price': rate,
                'side': side,
                'is_cancelled': False,
                'order_id': 0}

    @property
    def filename(self) -> str:
        return 'data/15m.pkl'

    def calc_fee(self):
        return 0.35
