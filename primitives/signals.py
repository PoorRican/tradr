from enum import IntEnum, unique


class Signal(IntEnum):
    """ Abstracts `Indicator` return output as a trinary decision.

    The available decisions are 'buy', 'hold', and 'sell'. In addition, 'buy'/'sell' decisions
    can be converted to a value of `Side`. Both discrete objects are needed to explicitly abstract
    indicator output and trade type. Boilerplate code is then reduced when checking signal value.

    Example:
        Convert value to `Side`:
            >>> from primitives import Side
            >>> signal = Side(signal)

        Check signal value:
            >>> if Signal:
            >>>     pass    # handle buy/sell
            >>> else:
            >>>     pass    # handle hold
    """
    SELL = -1
    HOLD = 0
    BUY = 1


@unique
class Side(IntEnum):
    """ Enumerates order types as buy/sell

    Used to characterize a market trade in a binary fashion.

    Notes:
        Values for `BUY`/`SELL` shall match corresponding values from `Signal`.
    """
    BUY = Signal.BUY
    SELL = Signal.SELL
