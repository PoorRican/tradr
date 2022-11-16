""" Abstracts core infrastructure of interacting with market platforms.

Encapsulates the ability to query market ticker data, and trade execution. In the future, both ticker data
functionality and exchange functionality will be separated. This is because for a given strategy or to characterize
market trends, multiple sources of ticker data might be referred to, but trades will be executed only one single
exchange.
"""
from core.market import Market
from core.markets import *
