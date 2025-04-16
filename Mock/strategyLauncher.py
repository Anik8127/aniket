from configparser import ConfigParser
import logicLevelExecute
from strategyTools.statusUpdater import statusManager


def run():
    configReader = ConfigParser()
    configReader.read('config.ini')

    algo_name = configReader.get('inputParameters', f'algo_name')

    strat_obj = logicLevelExecute.algoLogic(algo_name)
    
    '''
    Status Manager manages the main process of the strategy and also integrates it with algo Monitor.
    inputs:
            algoName: name of algo
            inputParams: parameters for strategy function
            stratObj: strategy object
            processList: list of process (used for multiprocessing... 1 process name is necessary)
    '''
    
    statusManager(algo_name, {}, strat_obj, ['process_1'])


if __name__ == '__main__':
    run()