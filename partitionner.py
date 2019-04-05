import pandas,geopandas,random,tqdm

from scipy.spatial                                import distance


places = []

chunksize = 1e5
i = 0
for chunk in tqdm.tqdm(pandas.read_csv('./data/train.csv',chunksize=chunksize,parse_dates     =['pickup_datetime'])):
    i +=1
    chunk.to_hdf('./data/train/{i}.h5'.format(i=i),index=None,key='table',mode='w')
    chunk.assign(
    
            dist = lambda DF : pandas.np.sqrt(pandas.np.sum(
                pandas.np.power(
                DF[['pickup_longitude','pickup_latitude']
                ].values - DF[
                ['dropoff_longitude','dropoff_latitude']].values,2),
                1)),
                dayofweek= lambda DF : DF.pickup_datetime.dt.dayofweek,
                dayofyear= lambda DF : DF.pickup_datetime.dt.dayofyear,
                hour     = lambda DF : DF.pickup_datetime.dt.hour,
                month    = lambda DF : DF.pickup_datetime.dt.month,
                weekofyear=lambda DF : DF.pickup_datetime.dt.weekofyear,
                year      = lambda DF : DF.pickup_datetime.dt.year).round({'pickup_latitude': 2, 'pickup_longitude': 2}).groupby(
                ['pickup_latitude','pickup_longitude','hour']
                ).key.count().reset_index(
                ).to_hdf('./data/traffic/{i}.h5'.format(i=i),index=None,key='table',mode='w')