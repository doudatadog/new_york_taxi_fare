#!/usr/bin/env python
# coding: utf-8

from utils import *



keepcols                                    = ['ntacode','ntaname','shape_area','shape_leng','pop_2000','pop_2010','evpop','geometry']
csvfilename                                 = 'data/New_York_City_Population_By_Neighborhood_Tabulation_Areas.csv'
geojsonfname                                = 'data/NTA map.geojson'
Rgeojsonfname                               = 'data/road.geojson'


pop_data                                    = pandas.read_csv(locate(csvfilename)).groupby(
                                              ['NTA Code','NTA Name']
                                              ).apply(lambda x :
                                              pandas.Series(
                                                {'pop_2000':x.query('Year==2000').Population.mean(),
                                                'pop_2010':x.query('Year==2010').Population.mean()})).reset_index()

shapes                                      = geopandas.read_file(locate(geojsonfname))

roads                                       = geopandas.read_file(
                                              locate(Rgeojsonfname)
                                              )[[u'physicalid',u'geometry']]


popshapes                                   = geopandas.GeoDataFrame(shapes.merge(
                                              pop_data,
                                              how ='left',
                                              right_on = 'NTA Code',
                                              left_on='ntacode'
                                                ).assign(
                                              evpop = lambda DF : (
                                                  DF.pop_2010 -DF.pop_2000
                                              )/DF.pop_2000
                                              ).fillna(0)[keepcols])


ids                                         = RightSpatialJoin(
    L_bounds                                = popshapes.geometry.apply(lambda x : x.bounds),
    R_bounds                                = roads.geometry.apply(lambda x : x.bounds))


popshapes['nb_roads']                       = ids.apply(lambda x : roads.loc[x].physicalid.unique().shape[0])

del roads,ids,shapes,pop_data

class GetFeatures:
    def __init__(
        self,
        filepath,
        popshapes                 = popshapes,
        readargs                  = readargs,
        writeargs                 = writeargs):
        self.writeargs            = writeargs
        self.filepath             = filepath
        self.df                   = pandas.read_hdf(filepath,**readargs).dropna()
        self.df                   = self.df.assign(
        geometry                  = [shapely.geometry.LineString([x,y])
                                     for x,y in
                                     tqdm.tqdm(zip(self.df[
                                         ['pickup_longitude','pickup_latitude']
                                     ].values,
                                                   self.df[
                                                       ['dropoff_longitude','dropoff_latitude']
                                                   ].values))]
        ).astype(
        {'pickup_datetime':'datetime64[ns]'}).assign(
                dist               = lambda DF : geopandas.GeoSeries(DF.geometry).length,
                dayofweek          = lambda DF : DF.pickup_datetime.dt.dayofweek,
                dayofyear          = lambda DF : DF.pickup_datetime.dt.dayofyear,
                hour               = lambda DF : DF.pickup_datetime.dt.hour,
                month              = lambda DF : DF.pickup_datetime.dt.month,
                 weekofyear        = lambda DF : DF.pickup_datetime.dt.weekofyear,
                        year       = lambda DF : DF.pickup_datetime.dt.year).reset_index(drop=True)
        self.numcols               = ['pop_2010','nb_roads']
        self.popshapes             = popshapes
    def matchGeoShapes(self):
            ids2                   = RightSpatialJoin(
            L_bounds               = self.df.geometry.apply(lambda x : x.bounds),
            R_bounds               = self.popshapes.geometry.apply(lambda x : x.bounds))
            result                 = self.popshapes[self.numcols].loc[[ h for x in ids2 for h in x]]
            result['row_id']       = [index
                                      for item in ids2.iteritems() 
                                      for index
                                      in [item[0]]*len(item[1])]
            self.result            = pandas.DataFrame({'row_id': [x for x in range(len(ids2))]}
            ).merge(result,how     ='left',on='row_id').fillna(0).pipe(
                lambda R           : pandas.DataFrame(
                 {'pop_2010_mean'  : R.groupby('row_id').pop_2010.mean(),
                  'nb_roads_mean'  : R.groupby('row_id').nb_roads.mean(),
                  'pop_2010_sum'   : R.groupby('row_id').pop_2010.sum(),
                 'nb_roads_sum'    : R.groupby('row_id').nb_roads.sum(),
                 'pop_2010_min'    : R.groupby('row_id').pop_2010.min(),
                 'nb_roads_min'    : R.groupby('row_id').nb_roads.min(),
                 'pop_2010_max'    : R.groupby('row_id').pop_2010.max(),
                 'nb_roads_max'    : R.groupby('row_id').nb_roads.max()}))
            pandas.concat(
                    [self.df,self.result]
                    ,axis=1).to_hdf(self.filepath,**self.writeargs)

#def main():
    #max_workers                     = 2
    #executor                        = ThreadPoolExecutor(max_workers=max_workers)
    #def Task(n,
    #    max_workers=max_workers)    :
    #    trainfiles                  = pandas.np.array_split(H5('train'),max_workers)[n].tolist()
    #    print(trainfiles)
    #    map(lambda f                : GetFeatures(f).matchGeoShapes(),trainfiles)
    #executor.map(lambda x : Task(x),range(max_workers))

if __name__ == '__main__'           : map(lambda f : GetFeatures(f).matchGeoShapes(),H5('train'))     