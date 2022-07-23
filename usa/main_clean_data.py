import pandas as pd

from usa.utils.metric_fund_rate import MetricWorkerFundRate
from usa.utils.metric_nasdaq import MetricWorkerNasdaq
from usa.utils.metric_nyse import MetricWorkerNyse
from usa.utils.metric_unemployment_rate import MetricWorkerUnemploymentRate
from utils.metric_inflation import MetricWorkerInflation
import utils.metric_simple_worker as msw
# INFLATION
# - inflation-all_items.csv
# INTEREST RATES
# - pre2003:
# -- FRB_H15 (5)-dwb-pre2003-fed-fund-rate.csv
# - post2003:
# -- FRB_H15 (4)-DWPC-post2003-fed-fund-rate.csv
# - NYSE
# -- ^NYA.csv
# - NASDAQ
# -- ^IXIC.csv
# EMPLOYMENT RATE
# - unemployment_rate.csv

# personal savings
# personal expenditures
# - consumer spending
# personal income
# mortgage amount
# consumer confidence
# market close
# - xjo
# government balance sheet in markets
# gdp
# Retail Sales
# Real Consumer Spending
# Fed Pace of Treasury Purchase Program
if __name__ == '__main__':
    mwi = MetricWorkerInflation()
    mwfr = MetricWorkerFundRate()
    mwur = MetricWorkerUnemploymentRate()
    mwnyse = MetricWorkerNyse()
    mwnasdaq = MetricWorkerNasdaq()
    classes = {
        'inflation-all_items': [mwi, ['clean_inflation_rate']],
        'FRB_H15 (4)-DWPC-post2003-fed-fund-rate': [mwfr, ['clean_fund_rate']],
        'FRB_H15 (5)-dwb-pre2003-fed-fund-rate.csv': [mwfr, ['clean_fund_rate']],
        'unemployment_rate': [mwur, ['clean_unemployment_rate']],
        '^NYA': [mwnyse, ['clean_nyse_close']],
        '^IXIC': [mwnasdaq, ['clean_nasdaq_close']],
    }
    # for _file, _items in classes.items():
    #     mc = _items[0]
    #     normalise_cols = _items[1]
    #
    #     raw_path = f'./data/raw/{_file}.csv'
    #     mc.raw_pd = msw.read_file(data_path=raw_path)
    #     mc.clean_data()
    #     inflation_save_path = f'./data/bronze/{_file}.parquet'
    #     mc.bronze_pd = mc.bronze_pd.reset_index()
    #     msw.save_clean(data_pd=mc.bronze_pd, save_path=inflation_save_path)
    #
    #     mc.silver_data()
    #     mc.silver_pd = msw.normalise_metric(data_pd=mc.silver_pd, metric=normalise_cols)
    #     inflation_save_path = f'./data/silver/{_file}.parquet'
    #     mc.silver_pd = mc.silver_pd.reset_index()
    #     msw.save_clean(data_pd=mc.silver_pd, save_path=inflation_save_path)

    # COMBINE FUND RATE
    fund_rate_files = ['./data/bronze/FRB_H15 (4)-DWPC-post2003-fed-fund-rate.parquet',
    './data/bronze/FRB_H15 (5)-dwb-pre2003-fed-fund-rate.parquet',]
    mwfr.combine_files(files_ls=fund_rate_files)
    mwfr.silver_pd = msw.normalise_metric(data_pd=mwfr.silver_pd, metric=['clean_fund_rate'])
    inflation_save_path = f'./data/silver/fund_rate_combined.parquet'
    mwfr.silver_pd = mwfr.silver_pd.reset_index()
    msw.save_clean(data_pd=mwfr.silver_pd, save_path=inflation_save_path)

    # GOLD JOIN DATA
    data_paths = [
        './data/silver/^IXIC.parquet',
        './data/silver/^NYA.parquet',
        './data/silver/fund_rate_combined.parquet',
        './data/silver/inflation-all_items.parquet',
        './data/silver/unemployment_rate.parquet',
    ]
    save_path = './data/gold/combined_metrics.parquet'
    combined_pd = msw.combine_metrics(data_ls=data_paths)
    msw.save_clean(data_pd=combined_pd, save_path=save_path)

    # PLOT DATA
    plot_path = './data/gold/combined_metrics.parquet'
    msw.plot_data(plot_path)
    print('bye')