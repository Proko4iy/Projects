# <YOUR_IMPORTS>
import datetime
import json
import os

import dill

import pandas as pd


def predict():
    #path='..'
    mod=sorted(os.listdir(f'/home/airflow/airflow_hw/data/models'))
    with open(f'/home/airflow/airflow_hw/data/models/cars_pipe_202403111310.pkl', 'rb') as file:
        model=dill.load(file)
    
    df_pred=pd.DataFrame(columns=['id','predict'])
    files_list=os.listdir(f'/home/airflow/airflow_hw/data/test')

    for filename in files_list:
        with open(f'/home/airflow/airflow_hw/data/test/{filename}') as file:
            form=json.load(file)
        data= pd.DataFrame.from_dict([form])
        prediction=model.predict(data)
        dict_pred={'id': data.id, 'pred':prediction}
        df=pd.DataFrame(dict_pred)
        df_pred=pd.concat([df,df_pred],axis=0)
        df=df_pred
    df.to_csv(f'/home/airflow/airflow_hw/data/predictions/{datetime.datetime.now().strftime("%Y%m%d%H%M")}.csv',index=False)



if __name__ == '__main__':
    predict()
