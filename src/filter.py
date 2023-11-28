
import pandas as pd
import datetime


def read_revenue_reveal_days(src_csv='營收發布.csv') -> dict:
    df = pd.read_csv(src_csv)

    companies = {}

    for i in range(len(df['證券代碼'])):
        company = df['證券代碼'][i][:4]
        try:
            revenue_reveal_day = str(int(df['營收發布日'][i]))
        except:
            pass

        if revenue_reveal_day.find('/')>-1:
            year = revenue_reveal_day.split('/')[0]
            month = revenue_reveal_day.split('/')[1]
            day = revenue_reveal_day.split('/')[2]
        else:
            year = int( revenue_reveal_day[:4])
            month = int(revenue_reveal_day[4:6])
            day =int( revenue_reveal_day[6:8])

        if companies.get(company) is None:
            companies[company] = {}
        if companies[company].get(year) is None:
            companies[company][year] = {}
        if companies[company][year].get(month) is None:
            companies[company][year][month] = {}

        companies[company][year][month] = day
    return companies


def find_k_valid_days_before_rev_day(k=5, year=2021, month=10, day=11)->list[datetime.date]:
    curr_day = datetime.date(year, month, day)
    while 1:
        if curr_day.strftime("%A") in ['Saturday', 'Sunday']:
            curr_day = curr_day - datetime.timedelta(days=1)
        else:
            break
    res = []
    while len(res) < 5:
        res.append(curr_day)
        curr_day = curr_day - datetime.timedelta(days=1)
        while 1:
            if curr_day.strftime("%A") in ['Saturday', 'Sunday']:
                curr_day = curr_day - datetime.timedelta(days=1)
            else:
                break
    return res


def filter_output(src_csv='output_1.csv',rev_day_csv='營收發布.csv'):
    df = pd.read_csv(src_csv)

    company_rev_days = read_revenue_reveal_days(rev_day_csv)

    for company in df.keys():
        if company == 'date':
            continue

        # generate valid days for this company
        valid_days = {}
        for year in company_rev_days[company]:
            for month in company_rev_days[company][year]:
                day = company_rev_days[company][year][month]
                valid_days_list=find_k_valid_days_before_rev_day(5,int(year),int(month),int(day))
                for valid_day in valid_days_list:
                    valid_days[str(valid_day.year)+'-'+str(valid_day.month)+'-'+str(valid_day.day)] = True

        dates = df['date']
        company_data = df[company]


        # if date is valid, add it to new csv
        filtered_dates = []
        filtered_data = []
        for i in range(len(dates)):
            year = int(dates[i].split('-')[0])
            month = int(dates[i].split('-')[1])
            day = int(dates[i].split('-')[2])
            date=str(year)+'-'+str(month)+'-'+str(day)
            if valid_days.get(date) ==1:
                filtered_dates.append(dates[i])
                filtered_data.append(company_data[i])

        df_save = pd.DataFrame({'date': filtered_dates,
                               'data': filtered_data}
                              )

        df_save.to_csv('%s_output_2.csv'%company)


if __name__ == '__main__':
    res = find_k_valid_days_before_rev_day(5, 2023, 11, 26)
    print(res)

    _=read_revenue_reveal_days()
    print(_["2308"])