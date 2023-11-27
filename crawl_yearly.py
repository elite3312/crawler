from src.get_keyword import get_keyword
from src.crawler import crawl_yearly
from config.config import *
if __name__ == '__main__':
    crawl_yearly(year,keywords,start_month,end_month,output1_path,wait_time) 
    