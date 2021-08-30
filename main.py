from ibapi.wrapper import EWrapper
from ibapi import utils
from ibapi.client import EClient, Contract
from ibapi.common import TickerId, BarData, TagValueList
from ibapi.utils import iswrapper
import rx
import rx.operators as rx_op
from rx.subject import AsyncSubject, Subject, BehaviorSubject, ReplaySubject
from rx.core.observable import Observable

from typing import List, Optional, Union, Dict, Tuple, Callable, Any
import datetime as dt
import psycopg2

import numpy as np
import pandas as pd

import logging


def make_contract(symbol: str, sec_type: str = "STK", currency: str = 'USD', exchange: str = 'SMART',
                  primaryExchange: str="ISLAND", localsymbol: str = None) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    contract.primaryExchange = primaryExchange
    if localsymbol:
        contract.localSymbol = localsymbol
    return contract

class AlphaClientWrapper(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, wrapper=self)
        EWrapper.__init__(self)
        self.request_id = 0
        self.started = False
        self.next_valid_order_id = None
        self.requests: Dict[int, Contract] = {}
        self._subjects: Dict[int, Subject[(Contract, BarData)]] = {}
        self.connected: BehaviorSubject[bool] = BehaviorSubject(False)

    def next_request_id(self, contract: Contract) -> int:
        self.request_id += 1
        self.requests[self.request_id] = contract
        return self.request_id

    def historicalDataRequest(self, contract: Contract, endDateTime:str,
                            durationStr:str, barSizeSetting:str, whatToShow:str = "TRADES",
                            useRTH:int = 0, formatDate:int = 1, keepUpToDate:bool = False) -> Observable:
        print('hist from contract', contract)
        print('historicalDataRequest %s, %s, %s, %s, %s, %s, %s' % (endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate))
        cid = self.next_request_id(contract) #, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
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
        # return subject
        return self._subjects[cid]

    @iswrapper
    def historicalData(self, reqId: int, bar: BarData) -> None:
        logging.info('historicalData %s, %s' % (reqId, bar))
        # print('historicalData %s, %s' % (reqId, bar))
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

    def do_connect(self, host: str = "127.0.0.1", port: int = 4001, clientId: int = 0) -> rx.Observable:
        self.connect(host, port, clientId)
        return self.connected

    @iswrapper
    def connectAck(self):
        print('conected 0')
        logging.info("Connected")
        self.connected.on_next(True)

    @iswrapper
    def connectionClosed(self):
        logging.info("Disconnected")
        self.connected.on_next(False)
        self.connected.on_completed()


    # @iswrapper
    # def nextValidId(self, order_id: int):
    #     super().nextValidId(order_id)
    #     # self.connected.on_next(True)


    @iswrapper
    def error(self, req_id: TickerId, error_code: int, error: str):
        super().error(req_id, error_code, error)
        print('error', error)
        err = Exception(f"Error. Id: {req_id} Code {error_code} Msg: {error}")
        if req_id < 0:
            logging.debug("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            # self.connected.on_error(err)
        else:
            logging.error("Error. Id: %s Code %s Msg: %s", req_id, error_code, error)
            subject = self._subjects[req_id]
            if (subject is not None):
                subject.on_error(err)

    def say_bye(self):
        print('bye!')
        self.disconnect()

def make_ib_contract_req(client: AlphaClientWrapper, endDateTime:str,
                        durationStr:str, barSizeSetting:str, whatToShow:str = "TRADES",
                        useRTH:int = 0, formatDate:int = 1, keepUpToDate:bool = False):
    def handle_conn_symbol(con_symbol: Tuple[bool, str]) -> Observable:
        _is_connected, symbol = con_symbol
        print('handle_conn_symbol', _is_connected, symbol)
        contract = make_contract(symbol)
        # now = dt.date.today()
        return client.historicalDataRequest(
            contract,
            endDateTime,
            durationStr,
            barSizeSetting,
            whatToShow,
            useRTH,
            formatDate,
            keepUpToDate
            # endDateTime=now.strftime('%Y%m%d 00:00:00'),
            # endDateTime="",
            # durationStr='1 M',
            # barSizeSetting='1 day',
        )
    return handle_conn_symbol


def make_ib_contract_req_plain(client: AlphaClientWrapper, symbol: str, endDateTime:str,
                        durationStr:str, barSizeSetting:str, whatToShow:str = "TRADES",
                        useRTH:int = 0, formatDate:int = 1, keepUpToDate:bool = False)  -> Observable:
    print('make_ib_contract_req_plain', symbol)
    contract = make_contract(symbol)
    # now = dt.date.today()
    return client.historicalDataRequest(
        contract,
        endDateTime,
        durationStr,
        barSizeSetting,
        whatToShow,
        useRTH,
        formatDate,
        keepUpToDate
        # endDateTime=now.strftime('%Y%m%d 00:00:00'),
        # endDateTime="",
        # durationStr='1 M',
        # barSizeSetting='1 day',
    )

def handle_ib_resp(table_name: str, conn: psycopg2._psycopg.connection):
    def handle_ib_response(con_data: Tuple[Contract, BarData]) -> Observable:
        cursor = conn.cursor()
        try:
            con, data = con_data
            date = dt.datetime.strptime(data.date, "%Y%m%d" if len(data.date) == 8 else "%Y%m%d  %H:%M:%S")
            # delete data if exists to keep fresh data and clean workflow
            cursor.execute(
                f'DELETE FROM {table_name} WHERE date = %s AND symbol = %s',
                (date, con.symbol)
            )
            cursor.execute(
                f'INSERT INTO {table_name} (date, symbol, exchange, open, high, low, close, barCount, volume, average) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (date, con.symbol, con.exchange, data.open, data.high, data.low, data.close, data.barCount, data.volume, data.average)
            )
            conn.commit()
            return rx.just((con, date))
        except Exception as ex:
            conn.rollback()
            return rx.throw(ex)
        finally:
            cursor.close()
    return handle_ib_response

def handle_conn_status(is_connected) -> Observable:
    if is_connected:
        return rx.just(True)
    else:
        return rx.throw(Exception("Client's not connected"))

def read_symbols_file(observer: rx.core.Observer, scheduler = None) -> rx.core.Observer:
    with open('symbols.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            observer.on_next(line)
        observer.on_completed()
    return observer

def read_symbols_arr() -> List[str]:
    with open('symbols.txt', 'r') as f:
        lines = f.readlines()
        return [line.replace('\n', '') for line in lines]

def run_v1():

    def on_final_results(con_data: Tuple[Contract, bool]):
        con, data = con_data
        print('on_final_results', con.symbol, type(data))
        print(con, data)

    client = AlphaClientWrapper()
    connection = client.do_connect(clientId=5)
    symbols =  rx.of('AAPL', 'TSLA', 'AMZN', 'MSFT', 'ABNB') # rx.create(read_symbols_file) #

    conn = psycopg2.connect('dbname=alpha user=postgres')
    # from psycopg2.pool import ThreadedConnectionPool

    def init_daily_reqs():
        connection \
            .pipe(
                rx_op.flat_map(handle_conn_status),
                rx_op.combine_latest(symbols),
                rx_op.flat_map(make_ib_contract_req(client, endDateTime="", durationStr="10 Y", barSizeSetting="1 day")),
                rx_op.merge(max_concurrent=2),
                rx_op.flat_map(handle_ib_resp('ib_bardata_1day', conn)),
                # rx_op.combine_latest(conn),

            ) \
            .subscribe(
                on_next=on_final_results, #lambda s: print(s),
                on_error=lambda s: print('err', s),
                # on_completed=lambda: client.say_bye()
            )

    init_daily_reqs()
    client.run()

def get_all_1daybar_bd_symbols(conn: psycopg2._psycopg.connection) -> List[str]:
    sql = "SELECT DISTINCT symbol FROM ib_bardata_1day"
    df = pd.read_sql(sql, conn)
    return df['symbol'].tolist()

def get_symbol_all_days(conn: psycopg2._psycopg.connection, symbol: str) -> List[dt.date]:
    sql = "SELECT DISTINCT date FROM ib_bardata_1day WHERE symbol = %s order by 1"
    df = pd.read_sql(sql, conn, params=(symbol,))
    return df['date'].tolist()

def fetch_symbol_1hourbar(client: AlphaClientWrapper):
    def handle_symbol(symbol: str, date: dt.datetime) -> rx.Observable:
        date_str = date.strftime("%Y%m%d 23:59:59")
        return make_ib_contract_req_plain(client, symbol, endDateTime=date_str, durationStr="1 D", barSizeSetting="1 hour")
    return handle_symbol

def main():
    client = AlphaClientWrapper()
    connection = client.do_connect(clientId=5)
    all_symbols = read_symbols_arr() #  ['ABNB'] # read_symbols_arr()[0:5] # rx.create(read_symbols_file) #

    conn = psycopg2.connect('dbname=alpha user=postgres')

    def on_success(res):
        print('on success', type(res), res)

    def on_error(err):
        print('on error', type(err), err)

    def on_complete():
        print('on complete')

    def get_db_bardaily_symbols() -> rx.Observable:
        symbols = get_all_1daybar_bd_symbols(conn)
        return rx.just(symbols)



    def init_daily_reqs(symbols):
        def run_flow(_: Any):
            def build_ibreq(symbol: str):
                return lambda _: make_ib_contract_req_plain(client, symbol, endDateTime="", durationStr="10 Y", barSizeSetting="1 day")
            obs_list = [
                rx.defer(build_ibreq(symbol))
                for symbol in symbols
            ]
            return rx.from_iterable(obs_list).pipe(
                rx_op.merge(max_concurrent=2),
                rx_op.flat_map(handle_ib_resp('ib_bardata_1day', conn)),
            )
        connection.pipe(
            rx_op.flat_map(handle_conn_status),
            rx_op.flat_map(run_flow),
        ) \
        .subscribe(
            on_next=on_success,
            on_error=on_error,
            on_completed=on_complete,
        )
    init_daily_reqs(all_symbols)
    client.run()



    # symbols.subscribe(
    #     on_next=lambda s: print(s)
    # )
    # rx.combine_latest()
    # client.connected.pipe(
    #     rx.
    # )


if __name__ == "__main__":
    main()
