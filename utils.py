import pandas,os,datetime
from scipy.spatial import distance


#this method will add path to data folder

path      = lambda  fp,name ='': './data/{0}/{1}'.format(*[fp,name])


#this method will extract any file with .h5 extension in a given folder of data
H5        = lambda  fp : [
                            path( 
                                fp   = fp,
                                name = name )
                                for name in os.listdir(path(fp=fp))
                                if name.split('.')[-1] == 'h5'
                          ]

#new york coordinates are within the following boundaries
lon_bnd   = [-75.,-71.]
lat_bnd   = [39.,42]
boundaryQ = '(@lon_bnd[0]<pickup_longitude<@lon_bnd[1]) and (@lat_bnd[0]<pickup_latitude<@lat_bnd[1])'
class Tic:
    def __init__(self)          : 
        self.tasks              = []
    def New_task(self,desc)          :
        self.desc               = desc
        print(desc)
        self.start              = datetime.datetime.now()
    def launch(self)            :
        self.Tdesc              = 'Lanched Process'
        print(self.Tdesc)
        self.Tstart              = datetime.datetime.now()
        return self
    def Toc(self):
        end                     = datetime.datetime.now()
        info                    = "Taskname : %s :\n\t\ttook me %s seconds" %(self.desc,end-self.start)
        self.tasks.append(info)
        print(info)
    def finish(self)            :
        self.start              = self.Tstart
        self.desc               = self.Tdesc
        self.Toc()
class FeatureEngineerer:
    def __init__(self,name) :
        self.name           = name
        self.pickgeo        = ['pickup_latitude','pickup_longitude']
        self.dropgeo        = ['dropoff_latitude','dropoff_longitude']
    def Read(self):
        self.DF             = pandas.read_hdf(
                              self.name,
                       key  = 'table'
        ).query(boundaryQ
        ).reset_index(drop=True
        ).drop(
        ['key','pickup_datetime'],
        axis=1
        ).assign(
        dist                 = lambda DF : pandas.np.sqrt(pandas.np.sum(
        pandas.np.power(
        DF[
        self.pickgeo
        ].values - DF[
        self.dropgeo
        ].values,2),1)) #computing efficiently Euclidean distance between pick and drop points
        )
    def MatchPopTraf(self,trafObj):
        self.DF                   = self.DF.pipe(
        lambda DF0                       : Link2DFS_by_nearestGeo(
        DF1                       = DF0,
        DF2                       = trafObj,
        ColMapper                 = [self.pickgeo,['lat','lng']],
        prefix                    = 'pickup')
        ).pipe(
        lambda DF1                : Link2DFS_by_nearestGeo(
        DF1                       = DF1,
        DF2                       = trafObj,
        ColMapper                 = [self.dropgeo,['lat','lng']],
        prefix                    = 'dropoff'))
    def Process(self,trafObj)     :
        clock                       = Tic().launch()
        clock.New_task(
        'Loading DF{}'.format(
        self.name))
        self.Read()
        clock.Toc()
        clock.New_task('Matching Pop')
        self.MatchPopTraf(trafObj)
        clock.Toc()
        clock.finish()
        return self.DF
def Link2DFS_by_nearestGeo(
	DF1,
	DF2,
	ColMapper,
	prefix):
	ncolsests 								 = 1
	D                                        = distance.cdist(XA=DF1[ColMapper[0]].values, XB=DF2[ColMapper[1]].values, metric='euclidean')
	idx                                      = pandas.np.argpartition(D, ncolsests)
	closestD								 = pandas.np.min(D,axis=1)
	idx                                      = idx[:,:ncolsests]
	picked 									 = DF2.loc[idx.flatten()
	].reset_index(
	drop=True
	).assign(Dist=closestD.flatten()
	).rename(
	columns									 = lambda x : '{0}_{1}'.format(prefix,x))
	result									 = pandas.concat([DF1,picked],axis=1)
	return 									   result