import storm
import numpy

class wrapper(storm.BasicBolt):
    def __init__(self, k=10, n_i=100, forget=.1):
        self.means = numpy.random.normal(size=(10,100))
        self._f = forget
    def process(self, tup):
        vector = numpy.array(tup.values[0])
        d = ( (self.means[:,:len(vector)] - vector)**2 ).sum(axis=1)
        idx = numpy.argmax(d)
        self.means[idx,:len(vector)] = (1.-self._f)*self.means[idx,:len(vector)] + self._f*vector
        storm.emit([d.tolist()])

wrapper().run()