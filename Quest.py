
from os import path
import pandas as pd
import numpy as np



panas_questionnaire = ['Active','Distressed','Interested','Inspired','Annoyed','Strong','Guilty','Scared','Hostile','Excited','Proud','Irritable','Enthusiastic','Ashamed','Alert','Nervous','Determined','Attentive','Jittery','Afraid','Stressed','Frustrated','Happy','Sad','(Angry)','(Irritated)',]
stai_questionnaire = ['I feel at ease','I feel nervous','I am jittery','I am relaxed','I am worried','I feel pleasant',]
sam_questionnaire = ['Valence','Arousal',]

class Quest:

    def __init__(self,S=None,url_base=None) -> None:
        self.__S = S if S else 'S10'

        self.__url_base =  url_base if url_base else 'https://raw.githubusercontent.com/arsen-movsesyan/springboard_WESAD/master/data/'
        self.__url_quest = f'{self.__url_base}/{self.__S}/{self.__S}_quest.csv'

        self.df_protocol = self.get_protocol()
        self.protocol = list(self.df_protocol.index)

        self.df_stai = self.get_stai()
        self.df_dim = self.get_dim()
        self.df_panas = self.get_panas()

    def get_protocol(self):
        df_quest_study_protocol_raw = pd.read_csv(self.__url_quest,skiprows=1,skipfooter=20,sep=';', engine='python').iloc[:,:6]
        df_quest_study_protocol_raw = df_quest_study_protocol_raw.set_index('# ORDER')

        f_split = lambda x: x.astype(str).str.split('.')
        f_transform_seconds = lambda col: [(int(item[0])*60)+int(item[1]) if len(item)==2 else int(item[0])*60 for item in col]

        df_quest_study_protocol = df_quest_study_protocol_raw.apply(f_split)
        df_quest_study_protocol = df_quest_study_protocol.apply(f_transform_seconds)
        df_quest_study_protocol = df_quest_study_protocol.T
        
        return df_quest_study_protocol

    def get_panas(self):
        df_quest_panas_raw = pd.read_csv(self.__url_quest,skiprows=4,skipfooter=14,sep=';',engine='python').T

        df_quest_panas_raw = df_quest_panas_raw.iloc[1:]
        df_quest_panas_raw = df_quest_panas_raw.reset_index(drop=True)
        
        df_quest_panas = df_quest_panas_raw.copy()
        
        df_quest_panas.columns = list(self.protocol)
        df_quest_panas['panas_questionnaire'] = panas_questionnaire
        
        return df_quest_panas
        
    def get_stai(self):
        df_quest_stai_raw = pd.read_csv(self.__url_quest,skiprows=10,skipfooter=8,sep=';',engine='python').T

        df_quest_stai_raw = df_quest_stai_raw.iloc[1:]
        df_quest_stai_raw = df_quest_stai_raw.reset_index(drop=True)

        df_quest_stai = df_quest_stai_raw.copy()
        df_quest_stai.columns = list(self.protocol)

        mask =~df_quest_stai['Base'].isna()
        
        df_quest_stai = df_quest_stai.loc[mask]
        df_quest_stai['stai_questionnaire'] = stai_questionnaire
        
        return df_quest_stai
        

    def get_dim(self):    
        df_quest_dim_raw = pd.read_csv(self.__url_quest,skiprows=16,skipfooter=2,sep=';',engine='python').T

        df_quest_dim_raw = df_quest_dim_raw.iloc[1:]
        df_quest_dim_raw = df_quest_dim_raw.reset_index(drop=True)

        df_quest_dim = df_quest_dim_raw.copy()
        df_quest_dim.columns = list(self.protocol)
        
        mask = ~df_quest_dim['Base'].isna()
        
        df_quest_dim = df_quest_dim.loc[mask]
        df_quest_dim['sam_questionnaire'] = sam_questionnaire
        return df_quest_dim
        
