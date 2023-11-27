### from pytrends.exceptions import ResponseError
from xmlrpc.client import ResponseError
from pytrends.request import TrendReq
from functools import partial
from datetime import date
from time import sleep
from calendar import monthrange
import pandas as pd
import numpy as np
import math


def get_last_date_of_month(year: int, month: int) -> date:
    return date(year, month, monthrange(year, month)[1])


def convert_dates_to_timeframe(start: date, stop: date) -> str:
    return f"{start.strftime('%Y/%m/%d')} {stop.strftime('%Y/%m/%d')}"


def _fetch_data(pytrends, build_payload, timeframe: str) -> pd.DataFrame:
    """Attempts to fecth data and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            build_payload(timeframe=timeframe)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True
    return pytrends.interest_over_time()


def scaler(unscaled_data):
    """Scale data to 0 ~ 100"""
    max_value = unscaled_data.max()
    min_value = unscaled_data.min()
    scaled_results = 100 * (unscaled_data - min_value) / (max_value - min_value)
    return scaled_results


def get_daily_data(word: str,
                 start_year: int,
                 start_mon: int,
                 stop_year: int,
                 stop_mon: int,
                 geo: str = 'TW',
                 verbose: bool = True,
                 wait_time: float=10.0) -> pd.DataFrame:

    # Set up start and stop dates
    start_date = date(start_year, start_mon, 1) 
    stop_date = get_last_date_of_month(stop_year, stop_mon)

    # Start pytrends for TW region
    pytrends = TrendReq(hl='zh-TW', timeout=(10,25), retries=10, backoff_factor=0.2, tz=-480)
    # Initialize build_payload with the word we need data for
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word], cat=0, geo=geo, gprop='')

    ### Obtain weekly data for all weeks in every 5 years  
    # 1. Split in groups (If the period is over 5 years, google trends will give monthly data instead.)
    year_gap = stop_year - start_year + 1
    if(year_gap <= 4):
        groups = 1 
    elif(year_gap == 5):
        groups = [1, 2][start_mon < stop_mon]
    else:
        groups = math.ceil((year_gap-5)/4)+1

    weekly = []
    # 2. Crawl weekly data in each groups
    for g in range(groups):
        date1 = ['{}-01-01'.format(start_year+4*g), start_date][g==0]
        date2 = ['{}-12-31'.format(start_year+4*(g+1)), stop_date][(g+1)==groups]  
        timeframe = f'{date1} {date2}'
        weekly.append(_fetch_data(pytrends, build_payload, timeframe))
        if verbose:
            print(f'{word}:Batch {g+1} ({timeframe}) processed.')  
        sleep(wait_time)
    
    # 3. Aggrgate all groups through comparable ratio calculated by the overlapping year
    for i in range(len(weekly)-1):
        intersect = [day for day in weekly[i].index if day in weekly[i+1].index]
        to_scale = weekly[i].loc[intersect][word]
        scale_by = weekly[i+1].loc[intersect][word]       
        strech_ratio = (scale_by.max() - scale_by.min()) / (to_scale.max() - to_scale.min())
        weekly[i][word] = scale_by.min() + strech_ratio * (weekly[i][word] - to_scale.min())
        weekly[i+1] = weekly[i].drop(intersect).append(weekly[i+1])
    
    weekly_trends = weekly[-1]
    weekly_trends[word] = scaler(weekly_trends[word])
    weekly_trends['isPartial'] = [True]*len(weekly_trends)   


    #### Get daily data, 6-month each time (should not exceed 270 days)
    # Note: If a timeout or too many requests error occur we need to adjust wait_time
    global results 
    results = {}
    half_year_counts = 0
    
    # 1-1. Period less than 1 year
    if(start_date.year == stop_date.year):
        if((start_date.month<=6) and (stop_date.month>6)):
            timeframe_first_half = '{} {}-06-30'.format(start_date, start_date.year)
            timeframe_second_half = '{}-07-01 {}'.format(stop_date.year, stop_date)
            results[timeframe_first_half] = _fetch_data(pytrends, build_payload, timeframe_first_half)
            sleep(wait_time)
            results[timeframe_second_half] = _fetch_data(pytrends, build_payload, timeframe_second_half)
        else:
            timeframe = convert_dates_to_timeframe(start_date, stop_date)
            results[timeframe] = _fetch_data(pytrends, build_payload, timeframe)  
            
    # 1-2. Period more than 1 year, or across 1 year        
    else:
        half_year_counts = [half_year_counts+1, half_year_counts+2][start_date.month<=6]
        half_year_counts = [half_year_counts+1, half_year_counts+2][stop_date.month>6]
        half_year_counts += (stop_date.year - start_date.year - 1) * 2     
        half_flag = 0
        year_record = start_date.year
        for i in range(1, half_year_counts+1):
            # Set timeframe of pytrend
            if(i == 1):
                # first period
                if(start_date.month<=6):
                    timeframe = '{} {}-06-30'.format(start_date, start_date.year)
                    half_flag = 1
                else:
                    timeframe = '{} {}-12-31'.format(start_date, start_date.year)
                    half_flag = 2
                    year_record += 1
            elif(i == half_year_counts):
                # last period
                if(stop_date.month<=6):
                    timeframe = '{}-01-01 {}'.format(stop_date.year, stop_date)
                else:
                    timeframe = '{}-07-01 {}'.format(stop_date.year, stop_date)
            else:
                # middle part
                if(half_flag == 1):
                    timeframe = '{}-07-01 {}-12-31'.format(year_record, year_record)
                    half_flag = 2
                    year_record += 1
                else:
                    timeframe = '{}-01-01 {}-06-30'.format(year_record, year_record)
                    half_flag = 1  
                    
            # Crawl Data
            results[timeframe] = _fetch_data(pytrends, build_payload, timeframe)
            if verbose:
                print(f'{word}:{timeframe} crawling.')            
            # If crawl fail (data is empty), retry 3 times
            re = 3
            while((len(results[timeframe])==0) and (re>0)):
                if verbose:
                    print(f'{word}:{timeframe} data missing. ###')
                results[timeframe] = _fetch_data(pytrends, build_payload, timeframe)
                re -= 1
                sleep(wait_time)
            
            sleep(wait_time)  # don't go too fast or Google will send 429s
     
    # 2. Aggregate daily data 
    daily = pd.concat(results.values()).drop(columns=['isPartial'])
    complete = daily.join(weekly_trends, lsuffix='_unscaled', rsuffix='_weekly')
      
    # 3. Adjust zero value and fill nan
    complete[f'{word}_weekly'].ffill(inplace=True)   # fill periodic values
    complete[f'{word}_unscaled'] = complete[f'{word}_unscaled'].replace(0, 1)
    complete[f'{word}_weekly'] = complete[f'{word}_weekly'].replace(0, 1)
    complete[f'{word}_unscaled'] = complete[f'{word}_unscaled'].fillna(0) 
    complete[f'{word}_weekly'] = complete[f'{word}_weekly'].fillna(0)  
    
    # 4. Scale daily data by weights so the data is comparable
    sum_tmp = 0
    sum_list = []
    for i in reversed(range(len(complete))): 
        if(np.isnan(complete.isPartial[i])):       
            sum_tmp += complete[f'{word}_unscaled'][i]
            sum_list.append(None)
        else:
            sum_tmp += complete[f'{word}_unscaled'][i] 
            sum_list.append(sum_tmp)
            sum_tmp = 0
    sum_list.reverse()
    complete['partial_sum'] = sum_list
    complete['partial_sum'].ffill(inplace=True)
    complete['partial_sum'] = complete['partial_sum'].fillna(1)  # denominator should not be 0
    complete[f'{word}'] = complete[f'{word}_weekly'] * complete[f'{word}_unscaled'] / complete['partial_sum']
    complete[f'{word}'] = scaler(complete[f'{word}'])
#    complete['result'] = complete[f'{word}_weekly'] * complete[f'{word}_unscaled'] / complete['partial_sum']
#    complete['result'] = scaler(complete['result'])
        
    return complete
def crawl_yearly(year,keywords,start_month,end_month,output_path):
    # In case of data miss result from 429s error, I save crawl data as a global variable
    global crawl_data_all, crawl_results
    
    year_1=year
    if start_month>=end_month:
        year_1=year+1

    crawl_data_all = pd.DataFrame()
    for word in keywords:
        crawl_results = get_daily_data(word, year, start_month, year_1, end_month, 'TW', True, wait_time=5.0)
        crawl_data_all[word] = crawl_results[word]
        print('---------- {} Finished ----------'.format(word))
        if(len(keywords)>1):
            sleep(20)
        
    crawl_data_all.to_csv(output_path, encoding='utf-8-sig')