from mrjob.job import MRJob

class MRCounter(MRJob):

    def mapper(self, __, line):
        __,__,age, no_friends = line.split(',')
        yield age, float(no_friends)
    
    def reducer(self, age, no_friends):
        total_friends = 0 
        no_ppl_per_age = 0
        for nf in no_friends:
            total_friends += nf
            no_ppl_per_age += 1
        yield age, total_friends / no_ppl_per_age

if __name__ == '__main__':
    MRCounter.run()