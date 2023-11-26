### 整理代號
import pandas as pd
import numpy as np
def get_keyword():
    df_R=pd.read_csv('營收公告.csv', thousands=',')
    df_R.drop(['年月'], axis=1, inplace=True)
    df_R.dropna(inplace=True)
    df_R.rename(columns={'證券代碼': 'name', '營收發布日': 'date', '單月營收成長率％': 'RS'}, inplace=True)
    df_R['name']=df_R['name'].str.split(' ', expand=True)[0]
    df_R['date']=df_R['date'].astype(int)
    df_R['date']=df_R['date'].astype(str)
    df_R['R_date']='V'
    df_R['date']=pd.to_datetime(df_R['date'])

    ### 篩選代號
    company=df_R.drop_duplicates(subset='name')
    company=list(company['name'])
    company.sort()
    code=company[101:103]
    # code_3.remove('2017')
    # code_3.remove('2020')
    # code_3.remove('2022')
    print(code)
    return code
    # print(len(company))
if __name__=='__main__':
    get_keyword()