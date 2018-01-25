class Preprocessing:
	@classmethod
	def preprocess (cls, collection, Stem=None, SpellCorrection=None, SentTokenize=None, WordTokenize=None, do_word_tokenize=True, do_sent_tokenize=True, do_stem=True, do_lemmatize=False, do_lower_case=True, has_tags=True, remove_stopword=False):
		tokens = None
		if has_tags:
			collection = cls._strip_tags (collection)
		if sent_tokenize:
			st = SentTokenize ()
			tokens = st.tokenize (collection, tokens)
		if do_word_tokenize:
			wt = WordTokenize ()
			tokens = wt.tokenize (collection, tokens) 
		if do_stem:
			s = Stem ()
			tokens = s.stem (tokens)
		if do_lower_case:
			tokens = cls._lower_case (tokens)
		if remove_stopword:
			tokens = cls._remove_stopword (tokens)

	@staticmethod
	def _lower_case (tokens):
		pass

	@staticmethod	
	def _strip_tags (collection):
		'''
		Documents from the server contain html tags within their content. Need to strip off the tage before other processes. 
		'''	

	def _remove_stopword (cls): pass

