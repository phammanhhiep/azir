from collections import defaultdict
import math

class Ranking: 
	'''
	The class have different implementation for scoring documents. However, all 
	assumes the web search query. In other words, assume each term in a query is 
	joined with another by an AND operator. 
	'''

	def __init__ (self, indexing=None, algorithm=None, algorithm_name='weightedZoneScore', training_data=None):
		self.indexing = indexing
		self._create_cache ()
		self._setup_contants ()
		if algorithm is None:
			if algorithm_name == 'weightedZoneScore':
				self._algorithm = WeightedZoneScoring.train (training_data)
		else:
			self._algorithm = algorithm

	def _setup_contants (self): pass

	def _create_cache (self): pass

	def get_algorithm (self):
		return self._algorithm

	def score (self, postings_lists):
		score = self._algorithm.score (postings_lists)
		return score

	def rank (self, scores): pass

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
			'DF_TOTAL': 0,
			'TFDF': 1,
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
		The format of postings_lists is [[docid, (df_total, (df, tf), (df, tf))], ...]

		::param postings_lists:: 
		'''
	
		SCORE = self.SCORE_LISTS['SCORE']
		DOCID = self.POSTING_LISTS['DOCID']
		TF_LIST = self.POSTING_LISTS['TF_LIST']
		DF_TOTAL = self.POSTING_LISTS['DF_TOTAL']
		TFDF = self.POSTING_LISTS['TFDF']
		DF = self.POSTING_LISTS['DF']
		TF = self.POSTING_LISTS['TF']
		scores = []

		for pl in postings_lists:
			docid = pl[DOCID]
			tf_list = pl[TF_LIST] 
			df_total = tf_list[DF_TOTAL]
			tfdf = tf_list[TFDF:]
			scores.append ([docid, 0])
			ascore = 0
			for df,tf in tfdf:
				ascore += (tf * math.log (df_total / df, 10))
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
			'DF_TOTAL': 0,
			'DVECTOR_LEN': 1,
			'TFDF': 2,
			'DF': 0,
			'TF': 1,
		}

		self.SCORE_LISTS = {
			'DOCID': 0,
			'SCORE': 1,
		}
	
	def score (self, postings_lists):
		'''
		
		::param postings_lists:: posting_lists has format as follow, 
			[[docid, (df_total, (df, tf), (df, tf))], ...]. 
			The first element of the list is data of the query, and thus has docid as None.
			The number of tuples for each list must be the same, since they 
			represent a common vector space.
		'''	

		def _tfidf (df_total, df, tf, base=10):
			return tf * math.log (df_total / df, base)

		SCORE = self.SCORE_LISTS['SCORE']
		QUERY = self.POSTING_LISTS['QUERY']
		DOC_LIST = self.POSTING_LISTS['DOC_LIST']
		DOCID = self.POSTING_LISTS['DOCID']
		TF_LIST = self.POSTING_LISTS['TF_LIST']
		DF_TOTAL = self.POSTING_LISTS['DF_TOTAL']
		DVECTOR_LEN = self.POSTING_LISTS['DVECTOR_LEN']
		TFDF = self.POSTING_LISTS['TFDF']
		DF = self.POSTING_LISTS['DF']
		TF = self.POSTING_LISTS['TF']
		scores = []
		query_pl = postings_lists[QUERY]
		query_tf_list = query_pl[TF_LIST]
		query_tfdf = query_tf_list[TFDF:]
		df_total = query_tf_list[DF_TOTAL]
		doc_pls = postings_lists[DOC_LIST:]

		query_weights = [_tfidf (df_total, df, tf) for (df,tf) in query_tfdf]

		for pl in doc_pls:
			docid = pl[DOCID]
			tf_list = pl[TF_LIST] 
			dvlen = tf_list[DVECTOR_LEN]
			tfdf = tf_list[TFDF:]
			scores.append ([docid, 0])
			weights = []
			for df,tf in tfdf:
				weights.append (_tfidf (df_total, df, tf))

			scores[-1][SCORE] = sum ([a * b for (a,b) in zip (weights, query_weights)]) / dvlen

		scores = sorted (scores, key=lambda x: x[SCORE], reverse=True)
		
		return scores	

	@classmethod
	def train (cls): pass		