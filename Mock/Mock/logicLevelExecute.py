from datetime import datetime
import time
from configparser import ConfigParser
import pandas as pd
from strategyTools import dataFetcher, reconnect
from strategyTools.infra import getCurrentExpiry
from strategyTools.statusUpdater import infoMessage, errorMessage, positionUpdator
from strategyTools.dataLogger import algoLoggerSetup
from strategyTools.tools import (getSym, symbolFinder, get_epoch, OHLCDataFetch)
from strategyInit import algoInit


'''
    strategyTools is the new library for common used methods and bypass the database connections through the library
    Common used methods includes
        postOrderToDbLIMIT,postOrderToDbSpread - For placing orders
        getCurrentExpiry,getNextExpiry - for fetching required expiries from the redis (getCurrentExpiry(expInd))
        getSym - to get a symbol of specific price (getSym('CE',expiry,priceFind,clientData))
        getCandleCount- fetch the candle count of the symbol in the database (getCandleCount(indice,1))
        infoMessage, errorMessage - print message in the algoMonitor (infoMessage(algoName = algoName, message=f'target'))
                                                                    (errorMessage(algoName=algoName,message=f'Process Error'))

        data = dataFetcher(self.symListConn)
        data, self.idMap, self.symListConn = reconnect(self.idMap, [indice])
        reconnect inputs only idMap and the list of symbols. Sock is removed
        
        getClientData - mongoclient for market data
        
        IMPORTANT - no DB connection will be made from the main logic except getClientData.
        dbConfig will be removed
'''


class algoFunction():
    def __init__(self, algo_name, entry_side, current_expiry, hedge_min_price, hedge_max_price, strategyLogger):
        self.algo_name = algo_name
        self.entry_allow = True
        self.strategyLogger = strategyLogger
        self.entry_side = entry_side
        self.hedge_min_price = hedge_min_price
        self.hedge_max_price = hedge_max_price
        self.stack = pd.DataFrame(columns=["Symbol", "CurrentPrice", "LastPrice"])
        self.last_sym = None
        self.idMap = {}
        self.symListConn = []
        self.current_expiry = current_expiry


    def update_stack(self, current_expiry, current_data, strike_dist):
        self.update_stack_price()
        for i in range(20):
            if self.entry_side == 'CE':
                symbol, strike = symbolFinder(strike_dist * (4-i), 'CE', strike_dist, current_data['Close'], current_expiry)
            elif self.entry_side == 'PE':
                symbol, strike = symbolFinder(strike_dist * (4-i), 'PE', strike_dist, current_data['Close'], current_expiry)
            
            if symbol not in self.stack['Symbol'].values:
                data, self.idMap, self.symListConn = reconnect(self.idMap, [symbol])
                if self.idMap[symbol] in data.keys():
                    new_row = {"Symbol": symbol, "CurrentPrice": data[self.idMap[symbol]], "LastPrice": data[self.idMap[symbol]]}
                self.stack.loc[len(self.stack)] = new_row
        self.stack = self.stack.sort_values(by="CurrentPrice",ascending=False).reset_index(drop=True)


    def update_stack_price(self):
        for index, row in self.stack.iterrows():
            data = dataFetcher(self.symListConn)
            self.stack.at[index, 'LastPrice'] = self.stack['CurrentPrice'][index]
            if self.idMap[row['Symbol']] in data.keys():
                self.stack.at[index, 'CurrentPrice'] = data[self.idMap[row['Symbol']]]

        self.stack = self.stack.sort_values(by="CurrentPrice",ascending=False).reset_index(drop=True)


    def find_closest_row(self, value, criteria):
        if criteria == 'greater':
            filtered_df = self.stack[self.stack['CurrentPrice'] >= value]
        elif criteria == 'lesser':
            filtered_df = self.stack[self.stack['CurrentPrice'] <= value]
        else:
            filtered_df = self.stack

        if filtered_df.empty:
            return None
        
        differences = (filtered_df['CurrentPrice'] - value).abs()
        if not differences.empty:
            idx_min = differences.idxmin()
            return idx_min
        else:
            return None


    def init_mountain(self, stack_point, lot_size, mountain_count, entryOrder):
        self.strategyLogger.info(self.stack.to_string)
        first_id = self.find_closest_row(stack_point, 'greater')
        self.margin_hedge(entryOrder, lot_size, mountain_count, 'margin_init')
        for i in range(mountain_count):
            sym_data = self.stack.loc[first_id + i].to_dict()
            position_info = {
                'Symbol': sym_data['Symbol'],
                'PositionStatus': -1,
                'EntryNature': 'main',
                'EntryPrice': sym_data['CurrentPrice'],
                'Quantity': lot_size,
                'SymID': self.idMap[sym_data['Symbol']]
            }
            entryOrder(position_info)
            self.strategyLogger.info(sym_data)
        self.entry_allow = False


    def re_init_mountain(self, stack_point, lot_size, mountain_count, openPnl, entryOrder):
        self.strategyLogger.info(f'reinitializing {self.entry_side}.....')
        first_id = self.find_closest_row(stack_point, 'lesser')
        i = 0
        while i < mountain_count:
            sym_data = self.stack.loc[first_id + i].to_dict()
            if not sym_data['Symbol'] in openPnl['Symbol'].values:
                position_info = {
                    'Symbol': sym_data['Symbol'],
                    'PositionStatus': -1,
                    'EntryNature': 'main',
                    'EntryPrice': sym_data['CurrentPrice'],
                    'Quantity': lot_size,
                    'SymID': self.idMap[sym_data['Symbol']]
                }
                entryOrder(position_info)
                self.strategyLogger.info(sym_data)
            else:
                self.strategyLogger.info(f"{sym_data['Symbol']} already exists")
            i+=1


    def scroll_position(self, stack_point, lot_size, entryOrder, openPnl):
        enter_pos = False
        crossed_stack_point = self.stack.loc[(self.stack['LastPrice'] > stack_point) & (self.stack['CurrentPrice'] <= stack_point)]
        for index, row in crossed_stack_point.iterrows():
            side_pos = openPnl.loc[(openPnl['Symbol'] == row['Symbol']) & (openPnl['EntryNature'] == 'main')]
            margin_pos = openPnl.loc[((openPnl['EntryNature'] == 'margin') | (openPnl['EntryNature'] == 'margin_init')) & (openPnl['Symbol'].str.contains(self.entry_side))]
            same_side_pos = openPnl.loc[(openPnl['EntryNature'] == 'main') & (openPnl['Symbol'].str.contains(self.entry_side))]

            if len(side_pos) < 1 or (side_pos['EntryPrice'][side_pos.index[-1]] > stack_point and len(side_pos) < 2):
                if len(margin_pos) <= len(same_side_pos):
                    self.margin_hedge(entryOrder, lot_size, 1, 'margin')
                position_info = {
                        'Symbol': row['Symbol'],
                        'PositionStatus': -1,
                        'EntryNature': 'main',
                        'EntryPrice': row['CurrentPrice'],
                        'Quantity': lot_size,
                        'SymID': self.idMap[row['Symbol']]
                    }
                entryOrder(position_info)
                self.strategyLogger.info(f'scrolling new position {self.entry_side}.....')
                self.strategyLogger.info(row.to_dict())
                self.last_sym = row['Symbol']
                enter_pos = True
        return enter_pos
    

    def max_leg_manager(self, max_legs, openPnl, exitOrder):
        sided_pos = openPnl.loc[(openPnl['Symbol'].str.contains(self.entry_side)) & (openPnl['EntryNature'] == 'main')]
        sided_pos['CurrentPrice'] = sided_pos['CurrentPrice'].astype(float)
        while len(sided_pos) > max_legs:
            idx_min = sided_pos['CurrentPrice'].idxmin()
            exit_pos_info = openPnl.loc[idx_min].to_dict()
            self.strategyLogger.info(f'max_leg_manager: {openPnl.loc[idx_min].to_dict()}')
            exit_type = "Target, Shift"
            exitOrder(idx_min, 'main', exit_pos_info['CurrentPrice'], exit_type)
            sided_pos = openPnl.loc[(openPnl['Symbol'].str.contains(self.entry_side)) & (openPnl['EntryNature'] == 'main')]
            sided_pos['CurrentPrice'] = sided_pos['CurrentPrice'].astype(float)


    def margin_hedge(self, entryOrder, lot_size, hedge_count, hedge_type):
        margin_sym = 'NotFound'
        hedge_try_count = 10
        price_find = self.hedge_min_price
        while margin_sym == 'NotFound' and price_find < self.hedge_max_price and hedge_try_count > 0:
            margin_sym = getSym(self.entry_side, self.current_expiry, price_find)
            price_find += price_find * 0.1
            hedge_try_count -= 1

        if margin_sym != 'NotFound':
            data, self.idMap, self.symListConn = reconnect(self.idMap, [margin_sym])
            for i in range(hedge_count):
                position_info = {
                        'Symbol': margin_sym,
                        'PositionStatus': 1,
                        'EntryNature':hedge_type,
                        'EntryPrice': data[self.idMap[margin_sym]],
                        'Quantity': lot_size,
                        'SymID': self.idMap[margin_sym]
                    }
                entryOrder(position_info)
            
            infoMessage(algoName=self.algo_name,
                    message=f"'{hedge_count} | {position_info['Symbol']} at {position_info['EntryPrice']} for qt {position_info['Quantity']}")
            time.sleep(3)


class algoLogic():
    def __init__(self, algo_name):
        self.config = ConfigParser()
        self.config.read("config.ini")

        self.idMap = {}
        self.symListConn = []

        self.today_date = datetime.now().date()
        
        self.algo_name = algo_name
        self.start_time = self.convert_str_time(self.config['inputParameters']['start_time'], self.today_date)
        self.end_time = self.convert_str_time(self.config['inputParameters']['end_time'], self.today_date)
        self.day_end_time = self.convert_str_time(self.config['inputParameters']['day_end_time'], self.today_date)

        self.underlying = self.config['inputParameters']['underlying']
        self.base_sym = self.config['inputParameters']['base_sym']
        self.strike_dist = int(self.config['inputParameters']['strike_dist'])
        self.max_loss_limit = int(self.config['inputParameters']['max_loss_limit'])
        self.lot_size = int(self.config['inputParameters']['lot_size'])
        self.re_evaluate_interval = int(self.config['inputParameters']['re_evaluate_interval'])
        self.sl_factor = float(self.config['inputParameters']['sl_factor'])
        self.stack_point = float(self.config['inputParameters']['stack_point'])
        self.level_1 = int(self.config['inputParameters']['level_1'])
        self.level_2 = int(self.config['inputParameters']['level_2'])
        self.level_3 = int(self.config['inputParameters']['level_3'])
        self.minus_factor = int(self.config['inputParameters']['minus_factor'])
        self.hedge_min_price = float(self.config['inputParameters']['hedge_min_price'])
        self.hedge_max_price = float(self.config['inputParameters']['hedge_max_price'])

        self.current_expiry = self.base_sym + getCurrentExpiry(self.base_sym)
        
        self.log_location, self.json_location = algoLoggerSetup(algo_name)
        self.today_date = datetime.now().date()


    def algo_position_update(self, openPnl, mpName):
        df = openPnl.copy()
        df['EntryTime'] = df['EntryTime'].astype(str)
        positionUpdator(df, mpName, self.algo_name)


    def update_current_price(self, index, data, openPnl):
        symbol = openPnl['Symbol'][index]
        if symbol not in self.idMap.keys():
            data, self.idMap, self.symListConn = reconnect(self.idMap, [symbol])
        openPnl.at[index, 'CurrentPrice'] = data[self.idMap[symbol]]
        openPnl.at[index, 'Pnl'] = (openPnl['CurrentPrice'][index] -
                                        openPnl['EntryPrice'][index]) \
                                        * openPnl['PositionStatus'][index] * \
                                        openPnl['Quantity'][index]
        return data
    

    def convert_str_time(self, time_string, today_date):
        parsed_time = datetime.strptime(time_string, "%H:%M:%S").time()
        combined_datetime = datetime.combine(today_date, parsed_time)
        epoch_time = combined_datetime.timestamp()
        return epoch_time


    def get_atm_straddle(self, current_data, strike_dist, current_expiry):
        idMap = {}
        call_sym, strike = symbolFinder(0, 'CE', strike_dist, current_data['Close'], current_expiry)
        put_sym, strike = symbolFinder(0, 'PE', strike_dist, current_data['Close'], current_expiry)
        data, idMap, symLis = reconnect(idMap, [call_sym, put_sym])
        today_range = data[idMap[call_sym]] + data[idMap[put_sym]]
        self.logger.info(f'today range is {today_range}')
        return round(today_range), round(data[idMap[call_sym]]), round(data[idMap[put_sym]])


    def get_mountain_legs(self,today_range):
        if today_range <= self.level_1:
            return 2
        elif today_range <= self.level_2:
            return 3
        elif today_range <= self.level_3:
            return 4
        else: return 5


    def get_max_loss_limit(self, today_range, lot_size, max_loss_limit):
        if today_range * lot_size > max_loss_limit:
            max_loss = max_loss_limit
        else:
            max_loss = today_range * lot_size

        self.logger.info(f'max_loss_limit is {-1 * (max_loss)}')
        return -1 * (max_loss)


    def interval_clock(self, interval):
        first_can_time = get_epoch([9,15])
        tempTi = int(datetime.now().timestamp())
        interval = interval * 60
        diff_in_time = tempTi - first_can_time
        remainder = diff_in_time % interval
        if remainder < 30:
            return True
        else:
            return False
        

    def parse_option_string(self, option_string, base_sym):
        option_type = option_string[-2:]
        expiry_start = len(base_sym)
        expiry_end = expiry_start + 7
        expiry = option_string[expiry_start:expiry_end]
        strike_price = option_string[expiry_end:-2]
        return expiry, strike_price, option_type
    

    def strikeFinder(self, symbol):
        call_put = symbol[-2] + 'E'
        strike = symbol[-7:-2]
        expiry = symbol.replace(strike,'')
        expiry = expiry.replace(call_put,'')
        return int(strike), expiry, call_put
    

    def exit_position(self, exit_type, index, row, exit_category, exitOrder):
        exitOrder(index, exit_category, row['CurrentPrice'], exit_type)
        self.logger.info(f'{exit_type} | {row}')


    def mainLogic(self, mpName):
        algo_obj = algoInit(self.log_location, self.json_location, self.algo_name, self.config, self.underlying)
        try:
            self.logger = algo_obj.logger
            
            OHLC = None
            last_candle_time = 0
            total_pnl = 0

            subcription_list = [self.underlying] + algo_obj.openPnl['Symbol'].tolist()
            data, self.idMap, self.symListConn = reconnect(self.idMap, subcription_list)

            entry_obj_ce = algoFunction(self.algo_name, 'CE', self.current_expiry, self.hedge_min_price, self.hedge_max_price, self.logger)
            entry_obj_pe = algoFunction(self.algo_name, 'PE', self.current_expiry, self.hedge_min_price, self.hedge_max_price, self.logger)

            while True:
                time.sleep(0.1)
                if time.time() < self.start_time + 1 or time.time() > self.end_time:
                    continue

                self.timestamp = str(datetime.fromtimestamp(time.time()))
                
                if self.idMap:
                    data = dataFetcher(self.symListConn)
                    for index, row in algo_obj.openPnl.iterrows():
                        data = self.update_current_price(index, data, algo_obj.openPnl)

                if not algo_obj.openPnl.empty:
                    total_pnl = round(algo_obj.openPnl['Pnl'].sum() + algo_obj.closedPnl['Pnl'].sum())
                    if total_pnl < algo_obj.jsonData['max_loss']:
                        for index, row in algo_obj.openPnl.iterrows():
                            self.exit_position("DAY OVER", index, row.to_dict(), 'only', algo_obj.exitOrder)
                            
                        self.logger.info(f"++++Max SL hit ending day++++ {total_pnl} | max_loss: {algo_obj.jsonData['max_loss']}")
                        algo_obj.jsonData['day_over'] = True
                
                if not algo_obj.openPnl.empty:
                    if time.time() >= self.day_end_time:
                        for index, row in algo_obj.openPnl.iterrows():
                            self.exit_position("Time Up", index, row.to_dict(), 'only', algo_obj.exitOrder)
                            algo_obj.jsonData['day_over'] = True

                OHLC,candle_flag,last_candle_time = OHLCDataFetch(self.underlying, self.start_time, last_candle_time, 1, 15, OHLC)
                if candle_flag:
                    current_data = OHLC.loc[OHLC.index[-1]].to_dict()
                    candle_time = str(datetime.fromtimestamp(OHLC.index[-1]))
                    
                    self.logger.info(f"----------------------------------------------------------------")
                    self.logger.info(f"time_stamp: {candle_time} | Open: {current_data['Open']} | Low: {current_data['Low']} |"
                                    f"High: {current_data['High']} | Close: {current_data['Close']}")

                    if self.interval_clock(self.re_evaluate_interval) or not algo_obj.jsonData['init_flag']:
                        today_range, call_price, put_price = self.get_atm_straddle(current_data, self.strike_dist, self.current_expiry)
                        algo_obj.jsonData['mountain_count'] = self.get_mountain_legs(today_range)
                        algo_obj.jsonData['max_legs'] = (algo_obj.jsonData['mountain_count'] * 2) - self.minus_factor
                        algo_obj.jsonData['max_loss'] = self.get_max_loss_limit(today_range, self.lot_size, self.max_loss_limit)
                        algo_obj.jsonData['init_flag'] = True
                        infoMessage(algoName=self.algo_name, 
                                    message=f"{current_data['Close']} | {today_range} ({today_range * self.lot_size}) | Legs: {algo_obj.jsonData['mountain_count']} | "
                                            f"C/P: {call_price}/{put_price} | MSL: {algo_obj.jsonData['max_loss']} | MTM: {total_pnl}")
                        algo_obj.write_json_data()

                    side_pos_pe = len(algo_obj.openPnl.loc[(algo_obj.openPnl['Symbol'].str.contains('PE')) & (algo_obj.openPnl['EntryNature'] == 'main')])
                    side_pos_ce = len(algo_obj.openPnl.loc[(algo_obj.openPnl['Symbol'].str.contains('CE')) & (algo_obj.openPnl['EntryNature'] == 'main')])
                    self.logger.info(f"mountain_count: {algo_obj.jsonData['mountain_count']} | max_legs: {algo_obj.jsonData['max_legs']} |"
                                    f"side_pos_ce: {side_pos_ce} | side_pos_pe: {side_pos_pe} | max_loss: {algo_obj.jsonData['max_loss']}")
                    self.logger.info(f"total_pnl: {total_pnl}")
                    entry_obj_ce.update_stack(self.current_expiry, current_data, self.strike_dist)
                    entry_obj_pe.update_stack(self.current_expiry, current_data, self.strike_dist)

                    if not algo_obj.openPnl.empty:
                        stop_loss_trigger = None
                        main_positions = algo_obj.openPnl.loc[(algo_obj.openPnl['EntryNature'] == 'main')]
                        for index, row in main_positions.iterrows():
                            expiry, strike_price, option_type = self.parse_option_string(row['Symbol'], self.base_sym)
                            if row["CurrentPrice"] >= row["EntryPrice"] * (1+self.sl_factor):
                                self.exit_position("Stoploss", index, row.to_dict(), 'main', algo_obj.exitOrder)
                                stop_loss_trigger = option_type
                    
                    # if not algo_obj.openPnl.empty:
                    #     if stop_loss_trigger == 'CE':
                    #         opp_side_pos = algo_obj.openPnl.loc[(algo_obj.openPnl['Symbol'].str.contains('PE')) & (algo_obj.openPnl['EntryNature'] == 'main')]
                    #         if len(opp_side_pos) < algo_obj.jsonData['mountain_count']:
                    #             entry_obj_pe.re_init_mountain(self.stack_point, self.lot_size, algo_obj.jsonData['mountain_count'],algo_obj.openPnl,algo_obj.entryOrder)
                        
                    #     elif stop_loss_trigger == 'PE':
                    #         opp_side_pos = algo_obj.openPnl.loc[(algo_obj.openPnl['Symbol'].str.contains('CE')) & (algo_obj.openPnl['EntryNature'] == 'main')]
                    #         if len(opp_side_pos) < algo_obj.jsonData['mountain_count']:
                    #             entry_obj_ce.re_init_mountain(self.stack_point, self.lot_size, algo_obj.jsonData['mountain_count'],algo_obj.openPnl,algo_obj.entryOrder)

                    if not algo_obj.jsonData['day_over']:
                        if algo_obj.jsonData['ce_mountain']:
                            entry_obj_ce.init_mountain(self.stack_point, self.lot_size, algo_obj.jsonData['mountain_count'], algo_obj.entryOrder)
                            algo_obj.jsonData['ce_mountain'] = entry_obj_ce.entry_allow
                        if algo_obj.jsonData['pe_mountain']:
                            entry_obj_pe.init_mountain(self.stack_point, self.lot_size, algo_obj.jsonData['mountain_count'], algo_obj.entryOrder)
                            algo_obj.jsonData['pe_mountain'] = entry_obj_pe.entry_allow

                        enter_pos_ce = entry_obj_ce.scroll_position(self.stack_point, self.lot_size, algo_obj.entryOrder, algo_obj.openPnl)
                        enter_pos_pe = entry_obj_pe.scroll_position(self.stack_point, self.lot_size, algo_obj.entryOrder, algo_obj.openPnl)

                        if enter_pos_ce:
                            entry_obj_ce.max_leg_manager(algo_obj.jsonData['max_legs'], algo_obj.openPnl, algo_obj.exitOrder)
                        if enter_pos_pe:
                            entry_obj_pe.max_leg_manager(algo_obj.jsonData['max_legs'], algo_obj.openPnl, algo_obj.exitOrder)

                    algo_obj.write_json_data()
                    self.algo_position_update(algo_obj.openPnl, mpName)
        except Exception as err:
            print(err)
            errorMessage(algoName=self.algo_name, message=f'{err}')
            self.logger.exception(err)