import pandas as pd
import numpy as np
from Quest import Quest

class Subject:

    def __init__(self,S=None,url_base=None) -> None:
        self.__url_base =  url_base if url_base else 'https://raw.githubusercontent.com/arsen-movsesyan/springboard_WESAD/master/data/'
        self.__S = S if S else 'S10'
        
        self.__url_subject = f'{self.__url_base}{self.__S}/{self.__S}_E4_Data'

        self.df_bvp_seconds = self.get_bvp()
        self.df_temp_seconds = self.get_temp()
        self.df_acc_seconds = self.get_acc()
        self.df_eda_seconds = self.get_eda()

        self.df_subject = pd.concat([self.df_bvp_seconds,self.df_eda_seconds,self.df_temp_seconds,self.df_acc_seconds],axis='columns')

        self.quest = Quest(S=self.__S)

    def get_bvp(self):
        url_bvp = f'{self.__url_subject}/BVP.csv'

        df_bvp_raw = pd.read_csv(url_bvp,header=None,skiprows=2,names=['bvp'])
        df_bvp_seconds = self.to_second(df_bvp_raw,hz=64)

        return df_bvp_seconds

    def get_temp(self):
        url_temp = f'{self.__url_subject}/TEMP.csv'

        df_temp_raw = pd.read_csv(url_temp,header=None,skiprows=2,names=['temp'])
        df_temp_seconds = self.to_second(df_temp_raw,hz=4)
        
        return df_temp_seconds

    def get_acc(self):
        url_acc = f'{self.__url_subject}/ACC.csv'
        
        df_acc_raw  = pd.read_csv(url_acc,header=None,skiprows=2,names=['acc_1','acc_2','acc_3'])
        df_acc_seconds = self.to_second(df_acc_raw,hz=32)
        
        return df_acc_seconds

    def get_eda(self):
        url_eda = f'{self.__url_subject}/EDA.csv'
        df_eda_raw = pd.read_csv(url_eda,header=None,skiprows=2,names=['eda'])
        df_eda_seconds = self.to_second(df_eda_raw,hz=4)

        return df_eda_seconds

    def to_second(self,df_raw,hz):
        df_seconds = pd.DataFrame()
        for col in df_raw:
            df_seconds[f'{col}_mean']  = df_raw.groupby(df_raw.index//hz)[col].mean()
            df_seconds[f'{col}_min']   = df_raw.groupby(df_raw.index//hz)[col].min()
            df_seconds[f'{col}_max']   = df_raw.groupby(df_raw.index//hz)[col].max()
            df_seconds[f'{col}_std']   = df_raw.groupby(df_raw.index//hz)[col].std()
            
        return df_seconds

    def get_all_protocol(self):
        return self.quest.protocol

    def search_protocol(self,protocol='Base'):
        df_subject = self.df_subject.copy()

        for _,row in self.quest.df_protocol.iterrows():
            if row.name != protocol: 
                continue
            
            start = row['# START']
            end = row['# END']

            df_slice = df_subject.iloc[start:end] 
            
        return df_slice