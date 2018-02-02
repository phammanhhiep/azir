from nltk import sent_tokenize

class SentTokenize:
	def __init__ (self): pass
	
	def tokenize (self, collection, tokens=None):
		'''
		::param tokens:: The tokens being produced. If exist, then return right away.

		'''		
		if tokens is None:
			tokens = sent_tokenize (collection)
		return tokens