from collections import defaultdict
import math

class Ranking: 
	'''
	The class have different implementation for scoring documents. However, all 
	assumes the web search query. In other words, assume each term in a query is 
	joined with another by an AND operator. 
	'''

	def __init__ (self, algorithm=None, algorithm_name='cs', training_data=None):
		self._create_cache ()
		self._setup_contants ()
		if algorithm is None:
			if algorithm_name == 'cs':
				self._algorithm = CosineScoring ()
			elif algorithm_name == 'wzs':
				self._algorithm = WeightedZoneScoring.train (training_data)
			elif algorithm_name == 'tfidf':
				self._algorithm = TFIDFScoring ()
		else:
			self._algorithm = algorithm

	def _setup_contants (self): pass

	def _create_cache (self): pass

	def get_algorithm (self):
		return self._algorithm

	def create_scoring_data (self, retrieval, postings_lists, query_tokens):
		'''
		Convert to correct form used by a score method.
		'''
		return self._algorithm.create_scoring_data (retrieval, postings_lists, query_tokens)	

	def score (self, scoring_data):
		'''
		Return scores of documents in decesdening order.
		'''
		score = self._algorithm.score (scoring_data)
		return score

class Scoring:
	'''
	The parent of all subsequence Scoring classes.
	Define interfaces and common methods.

	'''	

	def __init__ (self, weights=None):
		self._weights = weights

	def get_weights (self):
		return self._weights

	def set_weights (self, weights):
		if self._weights is None:
			self._weights = defaultdict (lambda: 0, {})
		if weights is not None:
			for k,v in weights.items ():
				self._weights[k] = v	

	def score (self): pass

class WeightedZoneScoring (Scoring):
	def __init__ (self, weights=None):
		Scoring.__init__ (self, weights)
		self._setup_contants ()

	def _setup_contants (self):
		self.POSTING_LISTS = {
			'DOCID': 0,
			'ZONELIST': 1,
		}

		self.SCORE_LISTS = {
			'DOCID': 0,
			'SCORE': 1,
		}

	def score (self, postings_lists):
		''' 
		Implement ranked Boolean retrieval (or weighted zone scoring).
		The structure of postings_lists used in the function may not the same as those used by other algorithms or used in indexing.

		ISSUE: 
			+ Need to reconsider the structure of postings lists used in the scoring algorithm
		
		::params postings_lists:: a list of postings lists, the structure is [[docid, [zone, zone, ..]], ...]

		'''
		scores = []
		DOCID = self.POSTING_LISTS['DOCID']
		ZONELIST = self.POSTING_LISTS['ZONELIST']
		SCORE = self.SCORE_LISTS['SCORE']
		for pl in postings_lists:
			docid = pl[DOCID]
			zones = pl[ZONELIST]
			scores.append ([docid, 0])
			ascore = 0
			for z in zones:
				ascore += self._weights[z]
			scores[-1][SCORE] = ascore
		scores = sorted (scores, key=lambda x: x[SCORE], reverse=True)
		return scores 	

	@classmethod
	def train (cls, data=None):
		'''
		Implementation for simple problems: only two zones for each document.
		Use default data if data is not provided.

		::return:: an instance of WeightedZoneScoring
		'''

		# PLACEHOLDER weights
		weights = {0: 0.1, 1: 0.2, 2: 0.5, 3: 0.2}
		weights = defaultdict (lambda: 0, weights)

		return WeightedZoneScoring (weights)

class TermFrequencyScoring (Scoring):
	'''
	Score documents with number of occurrence of all query terms in those documents.
	'''
	def __init__ (self, weights=None):
		Scoring.__init__ (self, weights)
		self._setup_contants ()

	def _setup_contants (self):
		self.POSTING_LISTS = {
			'DOCID': 0,
			'TF_LIST': 1,
		}

		self.SCORE_LISTS = {
			'DOCID': 0,
			'SCORE': 1,
		}


	def score (self, postings_lists):
		'''
		The format of postings_lists is [[docid, [tf, tf, ...]]]

		::param postings_lists:: 
		'''
	
		SCORE = self.SCORE_LISTS['SCORE']
		DOCID = self.POSTING_LISTS['DOCID']
		TF_LIST = self.POSTING_LISTS['TF_LIST']
		scores = []

		for pl in postings_lists:
			docid = pl[DOCID]
			tf_list = pl[TF_LIST]
			scores.append ([docid, 0])
			ascore = 0
			for f in tf_list:
				ascore += f
			scores[-1][SCORE] = ascore

		scores = sorted (scores, key=lambda x: x[SCORE], reverse=True)
		
		return scores

	@classmethod
	def train (cls, data=None): pass

class TFIDFScoring (Scoring):
	'''
	Score document using tf-idf method.
	'''

	def __init__ (self, weights=None):
		Scoring.__init__ (self, weights)
		self._setup_contants ()

	def _setup_contants (self):
		self.POSTING_LISTS = {
			'DOCID': 0,
			'TF_LIST': 1,
			'D': 0,
			'DFTF': 1,
			'DF': 0,
			'TF': 1,
		}

		self.SCORE_LISTS = {
			'DOCID': 0,
			'SCORE': 1,
		}


	def score (self, postings_lists):
		'''
		Calculate the score for each document in the postings_lists.
		The format of postings_lists is [[docid, (D, (df, tf), (df, tf))], ...]

		::param postings_lists:: 
		'''
	
		SCORE = self.SCORE_LISTS['SCORE']
		DOCID = self.POSTING_LISTS['DOCID']
		TF_LIST = self.POSTING_LISTS['TF_LIST']
		D = self.POSTING_LISTS['D']
		DFTF = self.POSTING_LISTS['DFTF']
		DF = self.POSTING_LISTS['DF']
		TF = self.POSTING_LISTS['TF']
		scores = []

		for pl in postings_lists:
			docid = pl[DOCID]
			tf_list = pl[TF_LIST] 
			D = tf_list[D]
			dftf = tf_list[DFTF:]
			scores.append ([docid, 0])
			ascore = 0
			for df,tf in dftf:
				ascore += (tf * math.log (D / df, 10))
			scores[-1][SCORE] = ascore

		scores = sorted (scores, key=lambda x: x[SCORE], reverse=True)
		
		return scores		

	@classmethod
	def train (cls): pass

class CosineScoring (Scoring):
	def __init__ (self, weights=None):
		Scoring.__init__ (self, weights)
		self._setup_contants ()

	def _setup_contants (self):
		self.POSTING_LISTS = {
			'QUERY': 0,
			'DOC_LIST': 1,
			'DOCID': 0,
			'TF_LIST': 1,
			'D_TOTAL': 0,
			'DOCV_LEN': 1,
			'DFTF': 2,
			'DF': 0,
			'TF': 1,
		}

		self.SCORE_LISTS = {
			'DOCID': 0,
			'SCORE': 1,
		}

	def _docv_len (self, docv, vocabulary, D):
		'''
		Calculate the document vector length given the vector.

		::param docv:: a dictionary {docid: xxx, tf: [(termid, tf), (termid, tf), ...]}
		'''
		TERMID = 0
		TF = 1
		tfs = docv['tf']
		weights = [self._tfidf (D, vocabulary[i[TERMID]]['df'], i[TF]) for i in tfs]
		return math.sqrt (sum ([a**2 for a in weights]))

	def create_scoring_data (self, retrieval, postings_lists, query_tokens):
		'''
		Query scoring data does not contain document vector length. An in the score function, the weight of each term in query vector is not normalized by the query vector length

		::return:: [[docid, (D, docv_len, (df, tf), (df, tf))], ...].
		'''	
		PL_DOCID = 0 
		PL_TF = 1
		TF_TID = 0 # within tf of a postings list
		TF_TF = 1
		
		D = retrieval.D	
		doc_data = []
		query_data = [None, D, None]
		vocabulary = retrieval.indexing.get_vocabulary ()
		query_count = defaultdict (lambda: {'df': None, 'tf': 0}, {})
		terms = list (set (query_tokens))
		termids = [vocabulary[t]['termid'] for t in terms]

		for t in query_tokens:
			query_count[t]['tf'] += 1

		for t in terms:
			df = vocabulary[t]['df']
			tf = query_count[t]['tf']
			query_data.append ((df, tf))

		docids = [d[PL_DOCID] for d in postings_lists]
		docvs = retrieval.indexing.get_doc_vector (docids)	
		tf_lists = [d[PL_TF] for d in postings_lists]

		for docid, tfs, docv in zip (docids, tf_lists, docvs):
			docv_len = self._docv_len (docv, vocabulary, D)
			doc_data.append ([docid, D, docv_len])
			doc_termids = [tf[TF_TID] for tf in tfs]

			for t,tid in zip (terms,termids):
				if tid in doc_termids:
					tindex = doc_termids.index (tid)
					df = vocabulary[t]['df']
					tf = tfs[tindex][TF_TF]
					doc_data[-1].append ((df, tf))
				else: 
					doc_data[-1].append ((1, 0)) # df=1, tf=0

		doc_data.insert (0, query_data)
		return doc_data

	def _tfidf (self, D, df, tf, base=10):
		return tf * math.log (D / df, base)	

	def score (self, scoring_data):
		'''
		
		::param scoring_data:: posting_lists has format as follow, 
			[[docid, (D, docv_len, (df, tf), (df, tf))], ...]. 
			The first element of the list is data of the query, and thus has docid as None.
			The number of tuples for each list must be the same, since they 
			represent a common vector space.
		'''	

		SCORE = self.SCORE_LISTS['SCORE']
		QUERY = self.POSTING_LISTS['QUERY']
		DOC_LIST = self.POSTING_LISTS['DOC_LIST']
		DOCID = self.POSTING_LISTS['DOCID']
		TF_LIST = self.POSTING_LISTS['TF_LIST']
		D_TOTAL = self.POSTING_LISTS['D_TOTAL']
		DOCV_LEN = self.POSTING_LISTS['DOCV_LEN']
		DFTF = self.POSTING_LISTS['DFTF']
		DF = self.POSTING_LISTS['DF']
		TF = self.POSTING_LISTS['TF']
		scores = []
		query_pl = scoring_data[QUERY]
		query_tf_list = query_pl[TF_LIST]
		query_tfdf = query_tf_list[DFTF:]
		D = query_tf_list[D_TOTAL] # total document number
		doc_pls = scoring_data[DOC_LIST:]

		query_weights = [self._tfidf (D, df, tf) for (df,tf) in query_tfdf]

		for pl in doc_pls:
			docid = pl[DOCID]
			tf_list = pl[TF_LIST] 
			docv_len = tf_list[DOCV_LEN]
			dftf = tf_list[DFTF:]
			scores.append ([docid, 0])
			weights = []
			for df,tf in dftf:
				weights.append (self._tfidf (D, df, tf))

			scores[-1][SCORE] = sum ([a * b for (a,b) in zip (weights, query_weights)]) / docv_len

		scores = sorted (scores, key=lambda x: x[SCORE], reverse=True)
		
		return scores	

	@classmethod
	def train (cls): pass		