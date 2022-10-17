from typing import Union

from markets.Market import Market


class SimulatedMarket(Market):
    """
    Mock class of `Market`.
    Has flat fee, always accepts an order.
    Todo:
        - Only accept trade 50% of the time. This adds realism to simulation.
    """

    def __init__(self, model: Market):
        super().__init__()

        self.model = model
        self.update()
        self.orders = 0

    def place_order(self, amount, rate, side) -> Union[dict, bool]:
        order = {'amount': amount,
                 'price': rate,
                 'side': side,
                 'is_cancelled': False,
                 'order_id': self.orders}
        self.orders += 1
        return order

    @property
    def filename(self) -> str:
        return self.model.filename

    def calc_fee(self):
        return self.model.calc_fee()

    def update(self):
        self.model.update()
        self.data = self.model.data
