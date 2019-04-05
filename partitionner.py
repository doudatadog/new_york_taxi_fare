#!/usr/bin/env python2

import pandas,tqdm


chunksize                  = 1e5 #Data will be loaded in chunks
i                          = 0 # iteration step

for chunk in tqdm.tqdm(

    pandas.read_csv(
        './data/train.csv'  ,
        chunksize           = chunksize,
        parse_dates         = ['pickup_datetime'] #Automatically parse input to Datetime64[ns] format
                        )   ):
    i +=1 #update step
    chunk.to_hdf(
        './data/train/{i}.h5'.format(
                            i=i),
                     index   =None,
                       key   ='table',
                      mode   ='w') #storing chunk to Hdf Format which is faster for read Ops
    chunk.assign(
    
            dist = lambda DF : pandas.np.sqrt(pandas.np.sum(
                pandas.np.power(
                DF[['pickup_longitude','pickup_latitude']
                ].values - DF[
                ['dropoff_longitude','dropoff_latitude']].values,2),
                1))            , #computing efficiently Euclidean distance between pick and drop points
                dayofweek      = lambda DF : DF.pickup_datetime.dt.dayofweek,
                dayofyear      = lambda DF : DF.pickup_datetime.dt.dayofyear,
                hour           = lambda DF : DF.pickup_datetime.dt.hour,
                month          = lambda DF : DF.pickup_datetime.dt.month,
                weekofyear      =lambda DF : DF.pickup_datetime.dt.weekofyear,
                year            = lambda DF : DF.pickup_datetime.dt.year).round(
                {'pickup_latitude': 2, 'pickup_longitude': 2}).groupby(
                ['pickup_latitude','pickup_longitude','hour']
                ).key.count().reset_index(
                ).to_hdf(
        './data/traffic/{i}.h5'.format(i=i),
                   index=None,
                     key='table',
                    mode='w') #Estimating Traffic per hour in pick up Area