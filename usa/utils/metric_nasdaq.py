import pandas as pd
class MetricWorkerNasdaq():
    def __init__(self):
        self.silver_pd = pd.DataFrame()
        self.bronze_pd = pd.DataFrame()
        self.raw_pd = pd.DataFrame()

    def clean_data(self):
        self.bronze_pd = self.raw_pd
        self.bronze_pd['clean_date'] = pd.to_datetime(self.bronze_pd['Date'], format='%Y-%m-%d')
        self.bronze_pd['clean_nasdaq_close'] = self.bronze_pd['Close']
        print('bye')

    def silver_data(self):
        self.silver_pd = self.bronze_pd[['clean_date', 'clean_nasdaq_close']]