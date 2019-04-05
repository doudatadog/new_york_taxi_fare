import multiprocessing as mp
import itertools
import csv

def worker(chunk):
    # `chunk` will be a list of CSV rows all with the same name column
    # replace this with your real computation
    print(len(chunk))
    return len(chunk)  

def keyfunc(row):
    # `row` is one row of the CSV file.
    # replace this with the name column.
    return row[0]

def main():
    pool = mp.Pool()
    largefile = './data/train.csv'
    num_chunks = 10
    results = []
    with open(largefile) as f:
        reader = csv.reader(f)
        chunks = itertools.groupby(reader, keyfunc)
        while True:
            # make a list of num_chunks chunks
            groups = [list(chunk) for key, chunk in
                      itertools.islice(chunks, num_chunks)]
            if groups:
                result = pool.map(worker, groups)
                results.extend(result)
            else:
                break
    pool.close()
    pool.join()
    print(results)

if __name__ == '__main__':
    main()