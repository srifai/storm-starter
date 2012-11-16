import storm
import numpy

class wrapper(storm.BasicBolt):

	def __init__(self):
		wordlist = set(" ".join(["the cow jumped over the moon",
		            "an apple a day keeps the doctor away",
	    		    "four score and seven years ago",
	        		"snow white and the seven dwarfs",
	        		"i am at two with nature"]).split(" "))
	
		self._n = len(wordlist)
		self.map = dict(zip(wordlist,range(self._n)))
	
	def process(self, tup):
		sentence = tup.values[0].lower()
		words = sentence.split(" ")
		bow = numpy.zeros((self._n,))
		for w in words:
			bow[self.map[w]]+=1   	
		storm.emit([bow.tolist()])

wrapper().run()