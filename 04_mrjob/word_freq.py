from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEX = re.compile(r"[\w']+")
class MRCounter(MRJob):

    def steps(self):
         return [
            MRStep(mapper = self.mapper_get_words,
                     reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_count_keys,
                   reducer = self.reducer_output_words)
         ]

    def mapper_get_words(self, __, line):
        words = WORD_REGEX.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer_count_words(self, word, occ):
        yield word, sum(occ)

    def mapper_count_keys(self, word, count):
        yield None, (count, word)
    
    def reducer_output_words(self, _, count_words):
        for count, word in sorted(count_words):
            yield int(count), word
if __name__ == '__main__':
    MRCounter.run()