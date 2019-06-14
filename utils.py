import pandas,os,datetime,geopandas,tqdm,dask.dataframe as dd,rtree,shapely
from concurrent.futures import ThreadPoolExecutor
from scipy.spatial import distance

tqdm.tqdm.pandas()

csvfilename                             = 'data/train.csv'
h5filename                              = 'data/full.h5'
trainpattern                            = lambda Id : 'data/train/{}.h5'.format(Id)
writeargs                               = {'index':None,'key':'table'}
readargs                                = {'key':'table'}

#this method will add path to data folder

path                                    = lambda  fp,name ='',root=os.getcwd(): '{0}/data/{1}/{2}'.format(*[root,fp,name])
locate                                  = lambda fn,root=os.getcwd() : '{r}/{f}'.format(r=root,f=fn)


#this method will extract any file with .h5 extension in a given folder of data
H5                                      = lambda  fp : [
                            path( 
                                fp   = fp,
                                name = name )
                                for name in os.listdir(path(fp=fp))
                                if name.split('.')[-1] == 'h5'
                          ]

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
def RightSpatialJoin(R_bounds,L_bounds):
        stream                                = ((i, b, None) for i, b in enumerate(R_bounds))
        tree_idx                              = rtree.index.Index(stream)
        idxmatch                              = L_bounds.apply(lambda x: list(tree_idx.intersection(x)))
        return idxmatch