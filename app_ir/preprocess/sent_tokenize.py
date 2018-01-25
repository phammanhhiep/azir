from nltk import sent_tokenize

class SentTokenize:
	def __init__ (self): pass
	
	def tokenize (self, collection):
		return sent_tokenize (collection)