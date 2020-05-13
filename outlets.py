import pymysql
import pandas as pd
from sqlalchemy import create_engine

engine=create_engine('mysql+pymysql://root:***@localhost/outlets')

outlets_df=pd.read_sql('SELECT * from outlets', engine)

#заммена комбинаций разделительных символов на пробел
outlets_df['Торг_точка']=outlets_df['Торг_точка_грязная'].str.replace('[\.\,\s;"]+', ' ', regex=True).str.strip()+' '+outlets_df['Торг_точка_грязная_адрес'].str.replace('[\.\,\s;"]+', ' ', regex=True).str.strip().str.rstrip('-')
outlets_df['Торг_точка_чистая']=outlets_df['Торг_точка_грязная']+' '+outlets_df['Торг_точка_грязная_адрес']

#поиск уникальных записей для каждого дубликата (дубликат - префикс уникальной записи)
for row in outlets_df.itertuples():
	df=outlets_df[outlets_df['Торг_точка'].str.startswith(str(row[6]))].query('id != '+str(row[1]))
	
	mx=df[df['Торг_точка'].map(len)==df['Торг_точка'].map(len).max()].sort_values('id')[:1]['id']

	if mx.size==1:
		outlets_df.loc[row[0], 'outlet_clean_id']=mx.values[0]

#поиск id уникальных записей, имеющих больше одного дубликата 
s=outlets_df['outlet_clean_id'].value_counts()
s2=s.where(s!=1).dropna()

#формирование Dataframe, содержащего уникальные записи (outlets_clean)
clean_df=outlets_df[~outlets_df['outlet_clean_id'].isin(s2.index)].drop_duplicates(['Торг_точка'])
clean_df=clean_df[~clean_df['outlet_clean_id'].isin(clean_df['id'])]
clean_df=clean_df.append(outlets_df[outlets_df['outlet_clean_id'].isnull()]).drop_duplicates(['Торг_точка'])
clean_df['new_id']=clean_df.reset_index().index.astype(pd.Int64Dtype())+1

#удаление лишних столбцов
clean_df=clean_df.drop(['Город дистрибьютора', 'Торг_точка_грязная', 'Торг_точка_грязная_адрес'], axis=1)
outlets_df=outlets_df.drop(['Торг_точка', 'Торг_точка_чистая'], axis=1)

#создание внешнего ключа для уникальных записей в outlets_df (сами на себя)
outlets_df.loc[outlets_df['outlet_clean_id'].isin(clean_df['outlet_clean_id']), 'outlet_clean_id']=None
outlets_df.loc[outlets_df['id'].isin(outlets_df['outlet_clean_id'].dropna()), 'outlet_clean_id']=outlets_df.loc[outlets_df['id'].isin(outlets_df['outlet_clean_id'].dropna()), 'id']

#переименование столбцов на "id_d" для внешнего соединения outlets_df и clean_df
clean_df.columns=['id_d', 'outlet_clean_id', 'Торг_точка', 'Торг_точка_чистая', 'new_id']
outlets_df.columns=['id', 'Город дистрибьютора', 'Торг_точка_грязная', 'Торг_точка_грязная_адрес', 'id_d']

#изменение типов
outlets_df['id_d']=outlets_df['id_d'].astype(pd.Int64Dtype())
clean_df['id_d']=clean_df['id_d'].astype(pd.Int64Dtype())

#left join
outlets_df=pd.merge(outlets_df, clean_df, on='id_d', how='left')

#приведение порядка и наименований стоолбцов к нужному виду
clean_df=clean_df.drop(['id_d', 'outlet_clean_id', 'Торг_точка'], axis=1)[['new_id', 'Торг_точка_чистая']]
clean_df.columns=['id', 'Торг_точка_чистый_адрес']
outlets_df=outlets_df.drop(['id_d', 'outlet_clean_id', 'Торг_точка', 'Торг_точка_чистая'], axis=1)
outlets_df.columns=['id', 'Город дистрибьютора', 'Торг_точка_грязная', 'Торг_точка_грязная_адрес', 'outlet_clean_id']

conn=engine.connect()
conn.execute("DELETE from outlets;")
conn.close()

clean_df.to_sql(name='outlets_clean', con=engine, if_exists='append', method='multi',index=False)
outlets_df.to_sql(name='outlets', con=engine, if_exists='append',  method='multi', index=False)
