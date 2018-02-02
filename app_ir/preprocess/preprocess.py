from app_ir.preprocess.word_tokenize import WordTokenize
from app_ir.preprocess.sent_tokenize import SentTokenize 

class Preprocessing:
	def __init__ (self):
		self._wt = WordTokenize ()
		self._st = SentTokenize () 
	
	def run (self, document, do_stem=True, do_lemmatize=False):
		tokens = self._tokenize (document)
		return tokens

	def _tokenize (self, document):
		'''
		::param document:: a string of terms.
		'''
		tokens = self._st.tokenize (document)
		tokens = [self._wt.tokenize (sent) for sent in tokens]
		return tokens

