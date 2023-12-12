# This is a script to continually update the candle data for the given assets.
# Data will be stored locally in the 'data' folder.

from apscheduler.schedulers.blocking import BlockingScheduler

from core import GeminiMarket

ASSETS = ('btcusd', 'ethusd', 'dogeusd', 'dogeusd', 'avaxusd')

def update_all():
    for asset in ASSETS:
        market = GeminiMarket(asset, '', '')

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    try:
        print('Starting...')
        scheduler.add_job(update_all, 'interval', seconds=30)
        scheduler.start()
    except KeyboardInterrupt:
        print("Shutting Down...")
        scheduler.shutdown()
