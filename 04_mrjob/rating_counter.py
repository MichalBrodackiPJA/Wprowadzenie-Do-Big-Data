from mrjob.job import MRJob

class MRCounter(MRJob):

    def mapper(self, __, line):
        __,__,rating, __ = line.split(',')
        yield rating, 1
    
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRCounter.run()