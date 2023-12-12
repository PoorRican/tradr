# This is a script to continually update the candle data for the given assets.
# Data will be stored locally in the 'data' folder.
from typing import Tuple, ClassVar

from apscheduler.schedulers.blocking import BlockingScheduler

from core import GeminiMarket


class AssetGroup(object):
    assets: ClassVar[Tuple[str, ...]] = ('btcusd', 'ethusd', 'dogeusd', 'dogeusd', 'avaxusd')

    instances: Tuple[GeminiMarket, ...]

    def __init__(self):
        self.instances = tuple(GeminiMarket(asset, '', '') for asset in self.assets)

    def update_all(self):
        for instance in self.instances:
            instance.update()

if __name__ == '__main__':
    group = AssetGroup()

    scheduler = BlockingScheduler()
    try:
        print('Starting...')
        scheduler.add_job(group.update_all, 'interval', seconds=30)
        scheduler.start()
    except KeyboardInterrupt:
        print("Shutting Down...")
        scheduler.shutdown()
