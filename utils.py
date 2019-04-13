import pandas,os
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

def Link2DFS_by_nearestGeo(
	DF1,
	DF2,
	ColMapper,
	prefix):
	ncolsests 								 = 1
	D                                        = distance.cdist(XA=DF1[ColMapper[0]].values, XB=DF2[ColMapper[1]].values, metric='euclidean')
	idx                                      = pandas.np.argpartition(D, ncolsests)
	closestD								 = pandas.np.argmin(D)
	idx                                      = idx[:,:ncolsests]
	picked 									 = DF2.loc[idx.flatten()
	].reset_index(
	drop=True
	).assgin(Dist=closestD
	).rename(
	columns=lambda DF : {
	x:'{0}_{1}'.format(prefix,x) 
	for x in DF.columns})
	result									 = pandas.concat([DF1,picked])
	return 									   result