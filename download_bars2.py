#!/usr/bin/env python3

import os
import sys
import argparse
import logging
from typing import List, Optional, Union, Dict, Tuple
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import rx
import rx.operators as ops
from rx.subject import AsyncSubject, Subject, BehaviorSubject, ReplaySubject
from rx.core.observable import Observable

from typing import List, Optional, NoReturn
from collections import defaultdict
from dateutil.parser import parse

import numpy as np
import pandas as pd

from ibapi import wrapper
from ibapi.common import TickerId, BarData
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.utils import iswrapper

ContractList = List[Contract]
BarDataList = List[BarData]
OptionalDate = Optional[datetime]


def make_download_path(args: argparse.Namespace, contract: Contract) -> str:
    """Make path for saving csv files.
    Files to be stored in base_directory/<security_type>/<size>/<symbol>/
    """
    path = os.path.sep.join([
        args.base_directory,
        args.security_type,
        args.size.replace(" ", "_"),
        contract.symbol,
    ])
    return path


class DownloadApp(EClient, wrapper.EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        wrapper.EWrapper.__init__(self)
        self.request_id = 0
        self.started = False
        self.next_valid_order_id = None
        # self.contracts = contracts
        # self.requests = {}
        # self.bar_data = defaultdict(list)
        # self.pending_ends = set()
        # # self.args = args
        # self.current = self.args.end_date
        # self.duration = self.args.duration
        # self.useRTH = 0

        self.requests: Dict[int, Contract] = {}
        self._subjects: Dict[int, Subject[(Contract, BarData)]] = {}
        self.connected: BehaviorSubject[bool] = BehaviorSubject(False)

        # self.engine = create_engine(self.args.db_url)

    def next_request_id(self, contract: Contract) -> int:
        self.request_id += 1
        self.requests[self.request_id] = contract
        return self.request_id

    def historicalDataRequest(self, contract: Contract, endDateTime:str,
                            durationStr:str, barSizeSetting:str, whatToShow:str = "TRADES",
                            useRTH:int = 0, formatDate:int = 1, keepUpToDate:bool = False) -> Observable:
        cid = self.next_request_id(contract)
        # self.pending_ends.add(cid)
        subject = Subject()
        self._subjects[cid] = subject

        self.reqHistoricalData(
            cid,  # tickerId, used to identify incoming data
            contract,
            endDateTime,  # always go to midnight
            durationStr,  # amount of time to go back
            barSizeSetting,  # bar size
            whatToShow,  # historical data type
            useRTH,  # useRTH (regular trading hours)
            formatDate,  # format the date in yyyyMMdd HH:mm:ss
            keepUpToDate,  # keep up to date after snapshot
            [],  # chart options
        )
        return self._subjects[cid]

    @iswrapper
    def historicalData(self, reqId: int, bar) -> None:
        logging.info('historicalData %s, %s' % (reqId, bar))
        print('historicalData %s, %s' % (reqId, bar))
        contract = self.requests[reqId]
        subject = self._subjects[reqId]
        if contract and subject:
            subject.on_next((contract, bar))

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        super().historicalDataEnd(reqId, start, end)
        logging.info('historicalDataEnd %s, %s, %s' % (reqId, start, end))
        print('historicalDataEnd %s, %s, %s' % (reqId, start, end))
        subject = self._subjects[reqId]
        subject.on_completed()

    @iswrapper
    def connectAck(self):
        logging.info("Connected")
        self.connected.on_next(True)

    @iswrapper
    def connectionClosed(self):
        logging.info("Disconnected")
        self.connected.on_next(False)
        self.connected.on_completed()

    @iswrapper
    def nextValidId(self, order_id: int):
        super().nextValidId(order_id)

        self.next_valid_order_id = order_id
        logging.info(f"nextValidId: {order_id}")
        # we can start now
        # self.start()

    @iswrapper
    def error(self, req_id: TickerId, error_code: int, error: str):
        super().error(req_id, error_code, error)
        err = Exception("Error. Id: %s Code %s Msg: %s" % req_id, error_code, error)
        if req_id < 0:
            logging.debug("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            self.connected.on_error(err)
        else:
            logging.error("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            # we will always exit on error since data will need to be validated
            subject = self._subjects[req_id]
            if (subject is not None):
                subject.on_error(err)
            # self.done = True

    def do_connect(self, host: str = "127.0.0.1", port: int = 4001, clientId: int = 0) -> rx.Observable:
        self.connect(host, port, clientId)
        return self.connected

    def say_bye(self):
        print('bye!')
        self.disconnect()

def make_contract(symbol: str, sec_type: str, currency: str, exchange: str,
                  primaryExchange: str, localsymbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    contract.primaryExchange = primaryExchange
    if localsymbol:
        contract.localSymbol = localsymbol
    return contract


def read_file(observer: rx.core.Observer, scheduler= None) -> None:
    # subject = ReplaySubject()
    with open('symbols.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            observer.on_next(line)
            # subject.on_next(line)
        # return subject
        observer.on_completed()



def main():

    app = DownloadApp()
    app.connect("127.0.0.1", clientId=1)

    app.run()


if __name__ == "__main__":
    main()

# download_bars.py --size "5 min" --start-date 20110804 --end-date 20110904 AAPL
# download_bars.py --size "1 day" --duration "1 Y" --end-date 20210808 ABNB
# stated @ 2021-08-04 23:35:45.267262
# end @ 2021-08-04 23:35:46.107792