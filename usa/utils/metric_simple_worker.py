import pandas as pd
from sklearn import preprocessing
from matplotlib import pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import numpy as np


def read_file(data_path: str):
    raw_pd = pd.read_csv(data_path)
    return raw_pd


def save_clean(data_pd: pd.DataFrame(), save_path:str):
    data_pd.to_parquet(save_path)


def combine_metrics(data_ls: list):
    data_dict = {}
    result_pd = pd.DataFrame({})
    for _i in data_ls:
        raw_pd = pd.read_parquet(_i)
        result_pd = pd.concat([result_pd, raw_pd[['clean_date']]], axis=0, ignore_index=True)
    result_pd = result_pd.drop_duplicates(subset=['clean_date'])
    result_pd = result_pd.sort_values(by='clean_date')
    result_pd = result_pd.set_index(keys='clean_date')
    for _i in data_ls:
        raw_pd = pd.read_parquet(_i)
        raw_pd = raw_pd.set_index(keys='clean_date').drop('index', axis=1)
        result_pd = result_pd.join(raw_pd, how='left')
    return result_pd


def normalise_metric(data_pd:pd.DataFrame(), metric: list):
    min_max_scaler = preprocessing.MinMaxScaler()
    for _i in metric:
        # data_pd[f'feat_{_i}'] = pd.DataFrame(min_max_scaler.fit_transform(pd.DataFrame(data_pd.loc[:,_i])))
        data_pd[f'feat_{_i}'] = min_max_scaler.fit_transform(data_pd[[_i]])
        data_pd = data_pd.drop(labels=[_i], axis=1)
    return data_pd

def plot_data(data_path:str):
    result_pd = pd.read_parquet(data_path)
    result_pd = result_pd.loc['2022-01-01':]

    # use_lines = False
    use_lines = True
    if use_lines:
        result_pd.interpolate(method='linear').plot.line()
        plt.show()
    else:
        pd.options.plotting.backend = "plotly"
        fig = px.line(result_pd, y=[
            'feat_clean_nasdaq_close',
            'feat_clean_nyse_close',
            'feat_clean_fund_rate',
            'feat_clean_inflation_rate',
            'feat_clean_unemployment_rate'
        ])
        # fig = result_pd.plot()
        fig.show()

