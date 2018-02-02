from nltk import word_tokenize
import string, re

class WordTokenize:
	def __init__ (self): pass
	
	def tokenize (self, sent=None):
		'''
		Process: 
			- Strip tags
			- Lower cases
			- Tokenize
			- Remove stopwords
		::param sent:: a string of terms in a sentence.
		'''
		if sent is None:
			raise ValueError ('No sentence is provided.')
		tokens = word_tokenize (sent)
		tokens = filter (None, [self._remove_special_character (t) for t in tokens])
		return list (tokens)

	def _remove_special_character (self, token):
		pattern = re.compile (r'[{}]'.format (re.escape (string.punctuation)))
		return pattern.sub ('', token)