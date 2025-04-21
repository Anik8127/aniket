import pandas as pd
import os
from datetime import datetime
import json
from strategyTools.dataLogger import setup_logger
from strategyTools.infra import (postOrderToDbLIMIT,postOrderToDbSpread,getBuyLimitPrice,getSellLimitPrice)
from strategyTools.statusUpdater import (infoMessage)



class algoInit:
    def __init__(self, log_location, json_location, algo_name, config_reader, underlying):
        self.algo_name = algo_name
        self.underlying = underlying
        self.config = config_reader
        self.openPnl = pd.DataFrame(
            columns=["SymID","EntryTime", "Symbol", "EntryPrice", "CurrentPrice", "Quantity", "PositionStatus", "Pnl", "EntryNature"])
        self.closedPnl = pd.DataFrame(columns=[
                                      "SymID","Key", "ExitTime", "Symbol", "EntryPrice", "ExitPrice", "Quantity", "PositionStatus", "Pnl", "ExitType", "EntryNature"])

        self.jsonData = {}
        self.fileDir = {
            "OpenPnlCsv": f"{json_location}/OpenPnlCSV/",
            "ClosePnlCsv": f"{json_location}/ClosePnlCSV/",
            "ClosePnlCsvLog": f"{log_location}/ClosePnlCSV/",
            "strategyLogs": f"{log_location}/{underlying}/",
            "algoJson": f"{json_location}/"
        }

        for dirs in self.fileDir.values():
            createDir = os.path.join(os.getcwd(), dirs)
            os.makedirs(createDir, exist_ok=True)

        self.today_date = datetime.now().date()
        self.open_pnl_suffix = self.today_date
        self.logger = setup_logger(underlying, f"{self.fileDir['strategyLogs']}logs_{self.today_date}.log")

        if self.config['executionParameters']['isLive'] == 'True':
            self.isLive = True
        else:
            self.isLive = False

        self.hedge_per = float(self.config['executionParameters']['hedge_limit_percent'])
        self.lowerPriceLimitPercent = float(self.config['executionParameters']['lowerPriceLimitPercent'])
        self.upperPriceLimitPercent = float(self.config['executionParameters']['upperPriceLimitPercent'])
        self.timeLimitOrder = float(self.config['executionParameters']['timeLimitOrder'])
        self.extraPercent = float(self.config['executionParameters']['extraPercent'])
        
        self.read_open_pnl_csv()
        self.read_close_pnl_csv()
        self.read_json_data()


    def read_json_data(self):
        try:
            file = open(f"{self.fileDir['algoJson']}{self.underlying}_{self.today_date}.json")
            self.jsonData = json.load(file)
            self.logger.info(f"JSON File Readed: {self.jsonData}")
        except Exception as e:
            self.logger.info(f"Error reading JSON File:\n {e}")
            self.jsonData = {
                'init_flag':False,
                'day_over': False,
                'mountain_count':0,
                'max_legs':0,
                'max_loss':0,
                'ce_mountain':True,
                'pe_mountain':True
            }
            self.write_json_data()


    def read_close_pnl_csv(self):
        try:
            close_csv_file = pd.read_csv(
                f"{self.fileDir['ClosePnlCsv']}{self.underlying}_{self.today_date}_closePosition.csv")
            if 'Unnamed: 0' in close_csv_file.columns:
                close_csv_file.drop(columns=['Unnamed: 0'], inplace=True)
            self.closedPnl = pd.concat([self.closedPnl, close_csv_file])

            self.closedPnl["Key"] = pd.to_datetime(self.closedPnl["Key"])
            self.closedPnl["ExitTime"] = pd.to_datetime(self.closedPnl["ExitTime"])

            self.logger.info(
                f"{self.fileDir['ClosePnlCsv']}{self.underlying}_{self.today_date}_closePosition.csv readed")
        except Exception as err:
            self.logger.warning(
                f"Could not read closePnlCsv of {self.underlying}_{self.today_date}")


    def write_json_data(self):
        print(f"{self.fileDir['algoJson']}{self.underlying}_{self.today_date}.json")
        try:
            with open(f"{self.fileDir['algoJson']}{self.underlying}_{self.today_date}.json", 'w') as json_file:
                json.dump(self.jsonData, json_file, indent=4)

            self.logger.info(f"JSON File Written: {self.jsonData}")
        except Exception as e:
            self.logger.info(f"Error writing JSON File:\n {e}")


    def read_open_pnl_csv(self):
        try:
            open_csv_file = pd.read_csv(
                f"{self.fileDir['OpenPnlCsv']}{self.underlying}_{self.open_pnl_suffix}_openPosition.csv")
            if 'Unnamed: 0' in open_csv_file.columns:
                open_csv_file.drop(columns=['Unnamed: 0'], inplace=True)
            self.openPnl = pd.concat([self.openPnl, open_csv_file])
            self.openPnl["EntryTime"] = pd.to_datetime(self.openPnl["EntryTime"])

            self.logger.info(
                f"{self.fileDir['OpenPnlCsv']}{self.underlying}_{self.open_pnl_suffix}_openPosition.csv readed")
        except Exception as err:
            self.logger.warning(
                f"Could not read openPnlCsv of {self.underlying}_{self.open_pnl_suffix}")


    def write_open_pnl_csv(self):
        self.openPnl.to_csv(
            f"{self.fileDir['OpenPnlCsv']}{self.underlying}_{self.open_pnl_suffix}_openPosition.csv")


    def write_close_pnl_csv(self):
        self.closedPnl.to_csv(
            f"{self.fileDir['ClosePnlCsv']}{self.underlying}_{self.today_date}_closePosition.csv")
        self.closedPnl.to_csv(
            f"{self.fileDir['ClosePnlCsvLog']}{self.underlying}_{self.today_date}_closePosition.csv")


    def entryOrder(self, position_info):
        if position_info['PositionStatus'] == -1: 
            position_status = 'SELL'
            if position_info['EntryNature'] == 'main':
                limit_price = getSellLimitPrice(position_info['EntryPrice'], self.extraPercent)
            else:
                limit_price = getSellLimitPrice(position_info['EntryPrice'], self.hedge_per)
            upperPriceLimitPercent = 0
            lowerPriceLimitPercent = self.lowerPriceLimitPercent
        elif position_info['PositionStatus'] == 1: 
            position_status = 'BUY'
            if position_info['EntryNature'] == 'main':
                limit_price = getBuyLimitPrice(position_info['EntryPrice'], self.extraPercent)
            else:
                limit_price = getBuyLimitPrice(position_info['EntryPrice'], self.hedge_per)
            upperPriceLimitPercent = self.upperPriceLimitPercent
            lowerPriceLimitPercent = 0

        postOrderToDbLIMIT(exchangeSegment='NSEFO',
                            algoName = self.algo_name,
                            isLive = self.isLive,
                            exchangeInstrumentID = position_info['SymID'],
                            orderSide = position_status,
                            orderQuantity = position_info['Quantity'],
                            limitPrice = limit_price,
                            upperPriceLimit = upperPriceLimitPercent*limit_price,
                            lowerPriceLimit = lowerPriceLimitPercent*limit_price,
                            timePeriod = self.timeLimitOrder,
                            extraPercent = self.extraPercent
                        )
        
        newTrade = pd.DataFrame({
            "SymID": position_info['SymID'],
            "EntryTime": datetime.now(),
            "Symbol": position_info['Symbol'],
            "EntryPrice": position_info['EntryPrice'],
            "CurrentPrice": position_info['EntryPrice'],
            "Quantity": position_info['Quantity'],
            "PositionStatus": position_info['PositionStatus'],
            "EntryNature": position_info['EntryNature'],
            "Pnl": 0
        }, index=[0])
        self.openPnl = pd.concat(
            [self.openPnl, newTrade], ignore_index=True)
        self.openPnl.reset_index(inplace=True, drop=True)
        self.logger.info(
            f"{position_status} Entry Order Executed in {position_info['Symbol']} @ {position_info['EntryPrice']}")
        if position_info['EntryNature'] == 'main':
            infoMessage(algoName=self.algo_name,
                    message=f"{position_status} | {position_info['Symbol']} at {position_info['EntryPrice']} for qt {position_info['Quantity']}")
        self.write_open_pnl_csv()


    def exitOrder(self, index, method, exit_price, exit_type):
        # Get the trade details from openPnl
        trade_to_close = self.openPnl.loc[index].to_dict()
        self.logger.info(trade_to_close)

        if method == 'main':
            limit_price = getBuyLimitPrice(exit_price, self.extraPercent)
            call_put = trade_to_close['Symbol'][-2:]
            newdfHedge = self.openPnl.loc[(self.openPnl['EntryNature'] == 'margin') & (self.openPnl['Symbol'].str.contains(call_put))]
            if not newdfHedge.empty:
                hedge_to_close = newdfHedge.loc[newdfHedge.index[-1]].to_dict()
                postOrderToDbSpread(exchangeSegment = 'NSEFO',
                                        algoName = self.algo_name,
                                        isLive = self.isLive,
                                        exchangeInstrumentID = trade_to_close['SymID'],
                                        orderSide = 'BUY',
                                        orderQuantity = int(trade_to_close['Quantity']), 
                                        limitPrice = limit_price,
                                        upperPriceLimit = self.upperPriceLimitPercent*limit_price,
                                        lowerPriceLimit = 0,
                                        timePeriod = self.timeLimitOrder,
                                        extraPercent = self.extraPercent,
                                        exchangeInstrumentIDSell = hedge_to_close['SymID']
                                        )
                self.close_trade(hedge_to_close, hedge_to_close['CurrentPrice'], 'hedge_close', newdfHedge.index[-1])
            else:
                postOrderToDbLIMIT(exchangeSegment = 'NSEFO',
                                        algoName = self.algo_name,
                                        isLive = self.isLive,
                                        exchangeInstrumentID = trade_to_close['SymID'],
                                        orderSide = 'BUY',
                                        orderQuantity = int(trade_to_close['Quantity']), 
                                        limitPrice = limit_price,
                                        upperPriceLimit = self.upperPriceLimitPercent*limit_price,
                                        lowerPriceLimit = 0,
                                        timePeriod = self.timeLimitOrder,
                                        extraPercent = self.extraPercent
                                        )
            self.close_trade(trade_to_close, exit_price, exit_type, index)

        elif method == 'only':
            if trade_to_close['PositionStatus'] == -1:
                limit_price = getBuyLimitPrice(exit_price, self.extraPercent)
                upperPriceLimit = self.upperPriceLimitPercent*limit_price
                lowerPriceLimit = 0
                orderSide = 'BUY'
            elif trade_to_close['PositionStatus'] == 1:
                limit_price = getSellLimitPrice(exit_price, self.extraPercent)
                upperPriceLimit = 0
                lowerPriceLimit = self.lowerPriceLimitPercent*limit_price
                orderSide = 'SELL'
            postOrderToDbLIMIT(exchangeSegment = 'NSEFO',
                                        algoName = self.algo_name,
                                        isLive = self.isLive,
                                        exchangeInstrumentID = trade_to_close['SymID'],
                                        orderSide = orderSide,
                                        orderQuantity = int(trade_to_close['Quantity']), 
                                        limitPrice = limit_price,
                                        upperPriceLimit = upperPriceLimit,
                                        lowerPriceLimit = lowerPriceLimit,
                                        timePeriod = self.timeLimitOrder,
                                        extraPercent = self.extraPercent
                                        )
            self.close_trade(trade_to_close, exit_price, exit_type, index)


    def close_trade(self, trade_to_close, exit_price, exit_type, index):
        self.openPnl.drop(index=index, inplace=True)
        self.write_open_pnl_csv()
        # Create a new row for closedPnl DataFrame
        trade_to_close['Key'] = trade_to_close['EntryTime']
        trade_to_close['ExitTime'] = datetime.now()
        trade_to_close['ExitPrice'] = exit_price
        # trade_to_close['PositionStatus'] = 1
        trade_to_close['Pnl'] = (trade_to_close['ExitPrice'] -
                                 trade_to_close['EntryPrice']) * trade_to_close['Quantity'] * trade_to_close['PositionStatus']
        trade_to_close['ExitType'] = exit_type

        for col in self.openPnl.columns:
            if col not in self.closedPnl.columns:
                del trade_to_close[col]

        # Append the closed trade to closedPnl DataFrame
        self.closedPnl = pd.concat(
            [self.closedPnl, pd.DataFrame([trade_to_close])], ignore_index=True)
        self.closedPnl.reset_index(inplace=True, drop=True)
        if trade_to_close['PositionStatus'] == 1:
            entry_side = 'BUY'
        else: entry_side = 'SELL'
        sl_percent = round(((trade_to_close['EntryPrice'] - trade_to_close['ExitPrice']) / trade_to_close['EntryPrice']) * 100)
        self.logger.info(f"{exit_type}: {entry_side} {trade_to_close['Symbol']} at {exit_price} | {sl_percent}")
        infoMessage(algoName=self.algo_name,
                    message=f"{exit_type}: {trade_to_close['Symbol']} at {exit_price} | {sl_percent}%")
        self.write_close_pnl_csv()