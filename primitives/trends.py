""" Detect and classify market trends.

Trends will influence trade decisions by modifying the amount traded, profit margins
"""
from enum import IntEnum
from dataclasses import dataclass


class TrendMovement(IntEnum):
    UP = 1
    CYCLE = 0
    DOWN = -1


@dataclass
class MarketTrend:
    trend: TrendMovement
    scalar: int = 1


