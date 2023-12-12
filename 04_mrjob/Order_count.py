from mrjob.job import MRJob

class MRCounter(MRJob):

    def mapper(self, __, line):
        client,__, order = line.split(',')
        yield client, float(order)
    
    def reducer(self, client, order):
        yield client, sum(order)

if __name__ == '__main__':
    MRCounter.run()