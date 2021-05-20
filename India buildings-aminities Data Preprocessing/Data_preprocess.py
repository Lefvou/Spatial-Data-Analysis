import pandas as pd 

df3 = pd.read_csv("amenity.csv", index_col=False)
df2 = pd.read_csv("building.csv", index_col=False)

df1 = df2['longitude-lattitude'].str[1:-1].str.split(',', expand=True).astype(float)
data = [df2['idA'], df1[0],df1[1]]
headers = ["IDA",'Longitude','Lattitude']
buildings1  = pd.concat(data, axis=1, keys=headers)

df4 = df3['longitude-lattitude'].str[1:-1].str.split(',', expand=True).astype(float)
data = [df3["idB"], df4[0],df4[1]]
headers = ["IDB",'Longitude','Lattitude']
amenity1 = pd.concat(data, axis=1, keys=headers)


amenity1.dropna(subset=["IDB",'Longitude','Lattitude'], inplace=True)
amenities = amenity1

buildings1.dropna(subset=["IDA",'Longitude','Lattitude'], inplace=True) 
buildings = buildings1


amenities.to_csv("amenities.csv", index=False)
buildings.to_csv("buildings.csv", index=False)