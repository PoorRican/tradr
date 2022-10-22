""" The `core` module abstracts interacting with asset markets via API\'s.

Interaction focuses on retrieving ticker data and executing trades. For simplicity and interoperability,
functionality is divided 4 ways:

-   Low-level interface for HTTP API requests and authentication are provided by `APIPrototype`, which provides
    minimum required methods and attributes for interacting with the platform. Specific implementations are stored
    in `core.proto`. Both `Market` and `Exchange` utilize these low-level methods and attributes.

-   Retrieving ticker orderbook data is handled by `Market`, which defines the minimum required interface for
    interacting with any market data. Specific implementations are stored in `core.markets`.

-   Trading is abstracted by `Exchange`, which defines the minimum required interface for sending and retrieving
    trade information. Specific implementations are stored in `core.exchanges`.

-   High-level implementation, where both `Market` and `Exchange` use the same `APIPrototype`. Specific implementations
    are stored in `core.platforms`.

Notes:
    `Market` functionality is encapsulated separately because a `Strategy` might use `Market` data from multiple sources
    to generate signals for a given asset, but then attempt to execute trades on a different `Exchange` entirely.
"""

from core.exchanges import *
from core.markets import *
from core.platforms import *
from core.proto import *

from core.api_proto import APIPrototype
from core.exchange import Exchange
from core.market import Market
