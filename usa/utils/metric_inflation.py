import pandas as pd
class MetricWorkerInflation():
    def __init__(self):
        self.bronze_pd = pd.DataFrame()
        self.raw_pd = pd.DataFrame()
        self.silver_pd = pd.DataFrame()

    def clean_data(self):
        self.bronze_pd = self.raw_pd
        change_col = '12-Month % Change'
        self.bronze_pd = self.bronze_pd.dropna(subset=[change_col])
        self.bronze_pd = self.bronze_pd[self.bronze_pd[change_col]!=' N/A']
        self.bronze_pd['clean_date'] = pd.to_datetime(self.bronze_pd['Label'], format='%Y %b')
        self.bronze_pd['clean_inflation_rate'] = self.bronze_pd[change_col]
        print('bye')

    def silver_data(self):
        self.silver_pd = self.bronze_pd[['clean_date', 'clean_inflation_rate']]

