""" run.py
Main file which implements `StocasticMACD` strategy,
sets up logging, and runs scheduler.
"""
from MarketAPI import GeminiAPI
from StochasticMACD import StochasticMACD
from investr import Investr
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

api_key = "account-n60VfGdUsMQmM3KUIPK2"
api_secret = "459WjMdQT6mWGYB1rxQejwi74wJs"
mark = GeminiAPI(api_key, api_secret)

amount = 0.000500    # $5 worth at time of writing
fiat = 50.0
minimum = 0.01
strat = StochasticMACD(amount, fiat, 0.10, minimum, mark)

inv = Investr(mark, strat)

logging.basicConfig(filename='investr.log', level=logging.INFO,
                    format='%(asctime)s|%(levelname)s|%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
logging.getLogger('apscheduler').setLevel(logging.ERROR)

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    try:
        logging.info("Starting...")
        scheduler.add_job(inv.run, 'interval', seconds=30)
        scheduler.add_job(inv.save, 'interval', minutes=1)
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Shutting Down")
        print("Shutting Down...")
        scheduler.shutdown()
