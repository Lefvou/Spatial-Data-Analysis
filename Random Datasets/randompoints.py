import pandas as pd
import numpy as np


#creating the dataframe
df1=pd.DataFrame()

#creating random points points between given lat lon
df1['x'] = np.random.randint(-180,180,size=(1000000))
df1['y'] = np.random.randint(-90,90,size=(1000000))


#creating id
df1['IDA'] =  df1.index +  1

#filtering the dataset
df1=df1[['IDA','x','y']]

#creating the dataframe
df2=pd.DataFrame()

#creating random points points between given lat lon
df2['x'] = np.random.randint(-180,180,size=(1000000))
df2['y'] = np.random.randint(-90,90,size=(1000000))

#creating id
df2["IDB"] = df2.index + 1

#filtering the dataset
df2=df2[['IDB','x','y']]

df1.to_csv("df1.csv",index=False)
df2.to_csv("df2.csv", index=False)
      