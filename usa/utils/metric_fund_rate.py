import pandas as pd
class MetricWorkerFundRate():
    def __init__(self):
        self.silver_pd = pd.DataFrame()
        self.bronze_pd = pd.DataFrame()
        self.raw_pd = pd.DataFrame()

    def clean_data(self):
        self.bronze_pd = self.raw_pd
        self.bronze_pd = self.bronze_pd.iloc[5:, :]
        value_col = 'The rate charged for primary credit under   amendment to the Board\'s Regulation A'
        if not value_col in self.bronze_pd.columns:
            value_col = 'AVERAGE DISCOUNT RATE ON LOANS TO MEMBER BANKS    QUOTED ON INVESTMENT BASIS    FEDERAL RESERVE BANK OF NEW YORK'
        time_col = 'Series Description'
        self.bronze_pd = self.bronze_pd[self.bronze_pd[value_col]!='ND']
        self.bronze_pd = self.bronze_pd.rename({value_col: 'clean_fund_rate', time_col: 'clean_time_period'}, axis=1)
        self.bronze_pd['clean_date'] = pd.to_datetime(self.bronze_pd['clean_time_period'], format='%Y-%m-%d')
        print('bye')

    def silver_data(self):
        self.silver_pd = self.bronze_pd[['clean_date', 'clean_fund_rate']]

    def combine_files(self, files_ls: list):
        result_pd = pd.DataFrame()
        for _file in files_ls:
            data_pd = pd.read_parquet(_file)
            data_pd = data_pd[['clean_date', 'clean_fund_rate']]
            result_pd = pd.concat([result_pd, data_pd], axis=0, ignore_index=True)
        result_pd = result_pd.reset_index(drop=True)
        result_pd = result_pd.sort_values('clean_date')
        self.silver_pd = result_pd
        print('bye')