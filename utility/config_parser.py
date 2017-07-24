import copy
from configparser import ConfigParser
import os

import time
import utility.cached_objects as co

#time.asctime( time.localtime(time.time() + 3600 * 24) )
#'Sun Jul 16 19:04:43 2017'

def get_config( section='application-config', config_type='app'):
    current_file_path = os.path.dirname(__file__)
    if config_type == 'app':
        file_name_path = '../config.ini'
        config_file_path = os.path.abspath(os.path.join(current_file_path, file_name_path))
        co.__app_config_dict = __validate(conf_dict=co.__app_config_dict, config_file_path=config_file_path,
                                             config_type=config_type, section=section)
        conf_dict = copy.copy(co.__app_config_dict)
    else:
        full_line_name = config_type + "_config_path"
        file_name_path = get_config()[full_line_name]
        config_file_path = os.path.abspath(os.path.join(current_file_path, file_name_path))
        if config_type == 'db':
            if section == 'mysql':
                co.__db_mysql_config_dict = __validate(conf_dict=co.__db_mysql_config_dict, config_file_path=config_file_path,
                                                     config_type=config_type, section=section)
                conf_dict = copy.copy(co.__db_mysql_config_dict)
        if config_type == 'stock':
            if section == 'generic-arguments':
                co.__stock_genericarguments_config_dict = __validate(conf_dict=co.__stock_genericarguments_config_dict, config_file_path=config_file_path, config_type=config_type, section=section)
                conf_dict = copy.copy(co.__stock_genericarguments_config_dict)
            if section == 'bitfinex':
                co.__stock_bitfinex_config_dict = __validate(conf_dict=co.__stock_bitfinex_config_dict, config_file_path=config_file_path,
                                                     config_type=config_type, section=section)
                conf_dict = copy.copy(co.__stock_bitfinex_config_dict)
            elif section == 'coinmarketcap':
                co.__stock_coinmarketcap_config_dict = __validate(conf_dict=co.__stock_coinmarketcap_config_dict, config_file_path=config_file_path,
                                                     config_type=config_type, section=section)
                conf_dict = copy.copy(co.__stock_coinmarketcap_config_dict)
            elif section == 'poloniex':
                co.__stock_poloniex_config_dict = __validate(conf_dict=co.__stock_poloniex_config_dict, config_file_path=config_file_path,
                                                     config_type=config_type, section=section)
                conf_dict = copy.copy(co.__stock_poloniex_config_dict)

    return conf_dict


def read_config(section, filepath='config.ini'):

    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filepath)

    # get section, default to mysql
    configs = {}
    configs['created_date'] = time.time()
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            configs[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filepath))

    return configs


def __is_cached(object, cache_age = (360 * 24 * 60 * 60 )):
    return ((len(object) > 1) and (time.time() <= object['created_date'] + cache_age))

def __validate(conf_dict, config_file_path, config_type, section):
    new_dict = copy.copy(conf_dict)
    if __is_cached(new_dict) is False:
        new_dict = read_config(filepath=config_file_path, section=section)
        print(config_type, 'file and', section,'section is re-cached')
    return new_dict