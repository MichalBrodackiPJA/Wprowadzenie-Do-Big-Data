from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCounter(MRJob):

    def steps(self):
         return [
            MRStep(mapper = self.mapper_get_order,
                     reducer = self.reducer_count_order),
            MRStep(mapper = self.mapper_count_keys,
                   reducer = self.reducer_output_order)
         ]

    def mapper_get_order(self, __, line):
        client,__, order = line.split(',')
        yield client, float(order)
    
    def reducer_count_order(self, client, order):
        yield client, sum(order)

    def mapper_count_keys(self, client, order):
        yield None, (order, client)
    
    def reducer_output_order(self, _, count_order):
        for order, client in sorted(count_order):
            yield int(order), client

if __name__ == '__main__':
    MRCounter.run()