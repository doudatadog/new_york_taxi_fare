#!/usr/bin/env python2

from utils import *
''' 
Reading train data

5GB train file is large, so we will first nee dask to quickly load it
Then we will save it in hdf file
After that we will partition this file into smaller chunks

'''
chunksize                               = 3e6 #Data will be loaded in chunks


df                                      = dd.read_csv(locate(csvfilename)) #quickly loads csv
df.to_hdf(
    locate(h5filename),
    **writeargs) #saving all data into one hdf file

del(df) # to free space

i                                       = 0 # iteration step


iterator                                =   pandas.read_hdf(
        locate(h5filename)  ,
        chunksize                       = chunksize,**readargs)    

for chunk in tqdm.tqdm(iterator)        :   
    i +=1 #update step
    chunk.to_hdf(
        locate(
        trainpattern(   Id              = i)),**writeargs) #storing chunk to Hdf Format which is faster for read Ops
os.remove(locate(h5filename))