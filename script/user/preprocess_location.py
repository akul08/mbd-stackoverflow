import pandas as pd

# read worldcities data
# source: https://simplemaps.com/data/world-cities
worldcities = pd.read_csv('simplemaps_worldcities_basicv1.4/worldcities.csv')

# preprocess
worldcities['city_ascii'] = worldcities['city_ascii'].fillna('').apply(lambda x: x.lower(),1)
worldcities['country'] = worldcities['country'].fillna('').apply(lambda x: x.lower(),1)
worldcities['admin_name'] = worldcities['admin_name'].fillna('').apply(lambda x: x.lower(),1)
worldcities['capital'] = worldcities['capital'].fillna('').apply(lambda x: True if x == 'primary' else False,1)

# save dataframe
df = worldcities[['city_ascii','country','admin_name','capital','lat','lng']]
df.rename(columns={"city_ascii":"city"},inplace=True)
df.to_csv('wordcities.csv',header=True,index=False)