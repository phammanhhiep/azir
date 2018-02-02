from collections import defaultdict

class Retrieval:
	'''
	Retrieve and rank results before return.
	Access indexes and vocabulary through indexing instance. This makes sense since indexing cache the whole vocabulary and part of the indexes.
	
	Assume web search. In other words, assume each term in a query is joined with another by an AND operator.
	
	::param indexing:: an instance of class Indexing.
	::param rank:: an instance of class Rank
	::param preprocessing:: an instance of class Preprocessing
	'''

	def __init__ (self, db=None, ranking=None, preprocessing=None, indexing=None):
		if None in [db, ranking, preprocessing, indexing]:
			raise ValueError ('Must provide ranking, preprocessing, and indexing')
		self.db = db
		self._content_coll = self.db.content_coll
		self.ranking = ranking
		self.preprocessing = preprocessing
		self.indexing = indexing
		self._create_cache ()
		self._setup_contants ()

	def _setup_contants (self):
		self.POSTINGS_LIST = {
			'DOCID': 0,
			'TF': 1, 
			'POSITIONS': 2,
		}

		# merged_indexes
	
	def _create_cache (self):
		self._count_docs ()	

	def _count_docs (self):
		self.D = self._content_coll.find_many ().count ()

	def _fetch_indexes (self, tokens):
		'''
		Get the indexes of the tokens
		'''

		tokens = list (set (tokens))
		vocabulary = self.indexing.get_vocabulary ()
		cached_indexes = self.indexing.get_indexes ()
		termids = [vocabulary[t]['termid'] for t in tokens]
		indexes = []
		fetched_termids = []		

		if len (cached_indexes) > 0:
			for t in termids:
				temp_pl = cached_indexes[t]
				if temp_pl is not None:
					indexes.append ({'termid':t, 'pl':temp_pl})
				else:
					fetched_termids.append (t)
		else:
			fetched_termids = termids
		
		disk_indexes = list (self.indexing.fetch_indexes (fetched_termids))
		indexes += disk_indexes
		indexes = sorted (indexes, key=lambda x: len (x['pl']))
		return indexes

	def _fetch_docs (self, postings_lists): pass

	def fetch_docs (self, docids):
		'''
		::param docids:: 
		::return:: a list of list [docid, doc]

		'''
		docs = self._content_coll.find_many (docids)
		results = [[d['_id'], d['content']] for d in docs]
		return results

	def _match_docids (self, indexes):
		'''
		Find a list docids which is the intersection of sets of docids associated with documents contain the terms.
		Assume that indexes has length being at least 2.
		::return:: A list of docid
		'''
		num = len (indexes)
		docids = set ([d[0] for d in indexes[0]['pl']])
		for i in range (num-1):
			next_pl = indexes[i+1]['pl']
			next_docids = [d[0] for d in next_pl]
			docids = docids.intersection (next_docids)
		return docids

	def _create_doc_indexes (self, indexes, matched_docids):
		'''
		From term indexes, create doc indexes, in which each element contain tuples of (term,position)

		::return:: document indexes, whose structure is following,
		{docid: [(termid, position),], }
		'''

		POSITIONS = self.POSTINGS_LIST['POSITIONS']
		DOCID = self.POSTINGS_LIST['DOCID']
		TF = self.POSTINGS_LIST['TF']
		matched_indexes = []
		doc_indexes = defaultdict (lambda: [],{})

		for index in indexes: 
			pls = index['pl']
			matched_pls = [p for p in pls if p[DOCID] in matched_docids]
			matched_indexes.append ({'termid': index['termid'], 'pl': matched_pls})	

		for index in matched_indexes:
			pls = index['pl']
			termid = index['termid']
			for pl in pls:
				docid = pl[DOCID]
				positions = pl[POSITIONS:]
				tf = pl[TF]
				[doc_indexes[docid].append ((termid, tf, p)) for p in positions]
		return doc_indexes

	def _merge_indexes (self, indexes, k=100):
		'''
		Merge indexes into postings list for each documents.
		Assume the postings lists are sorted according to doc frequency.
		Group the term positions into lists corresponding to each document.
		Sort term postions according to the term positions values.
		
		For each of new position lists, process from left to right (low postion to high postion),
		obtain a lists of postions that meet the below requirements:
			- Two consecutive term postions must be close to each other within k distance
			- No term has two instances being place next to each other without any instance 
			of other term being placed between them. <<< NOTICE: The condition should be used 
			only if the system cannot tokenize words well enough, since it is possible to 
			condition part of a term as an individual term and return misleading results. 
			For example like "New York Times", with poor word tokenize tool, each is treated 
			as individual word and without the condition, the relevance of results will be low. 
			Once the confidence in word tokenization is high, should not enforce the condition. >>>
		
		The result posting lists are ranked in descending order according to how many 
		query token each document contains.	
		
		FUTURE WORK:
		- Consider to enfore another condition: the order of between the positions of two 
		terms match with order of corresponding query terms. However, a user may enter 
		keywords rather than a sentence, and thus the order may not so important. 

		::param postings lists:: a list of postings in the same form created by instance of class Indexing
		::param k:: Max number of distanace between consecuitive term in query. Default is very large distance to ensure that such requirement is not enfored. 
		::return:: postings lists for each valid document, whose structure as following,
		[[docid, [(termid, tf), (termid, tf), ...], [position, ...]], ...]
		'''	

		DOCID = self.POSTINGS_LIST['DOCID']
		POSITIONS = self.POSTINGS_LIST['POSITIONS']
		TF = self.POSTINGS_LIST['TF']
		TP_TERMID = 0 
		TP_TF = 1
		TP_POSITION = 2
		results = []
		plnum = len (indexes)
		if plnum == 0: pass # no keywords found
		elif plnum == 1: # only one keywords
			index = indexes[0]
			pls = index['pl']
			termid = index['termid']
			results = [[p[DOCID], (termid, p[TF]), p[POSITIONS:]] for p in pls]
		else: # more than one keywords found
			matched_docids = self._match_docids (indexes)
			if len (matched_docids) > 0: # the two terms occur in the some common docs
				doc_indexes = self._create_doc_indexes (indexes, matched_docids)
				for docid, term_positions in doc_indexes.items ():
					term_positions = sorted (term_positions, key=lambda x: x[TP_POSITION])
					num = len (term_positions)
					matched_positions = set ()
					term_tfs = set ()
					for i in range (num-1):
						cur_tp = term_positions[i]
						next_tp = term_positions[i + 1]
						cur_term_tf = cur_tp[:TP_POSITION]
						next_term_tf = next_tp[:TP_POSITION]
						cur_p = cur_tp[TP_POSITION]
						next_p = next_tp[TP_POSITION]
						offset = next_p - cur_p

						if cur_tp[TP_TERMID] == next_tp[TP_TERMID]: continue 
						if offset <= k and offset > 0:
							term_tfs.add (cur_term_tf)
							term_tfs.add (next_term_tf)
							matched_positions.add (cur_p)
							matched_positions.add (next_p)
					matched_positions = sorted (matched_positions)
					term_tfs = sorted (term_tfs, key=lambda x: x[0]) # sorted by term. For testing.
					results.append ([docid, term_tfs, matched_positions])
		
		docids = [d[0] for d in results]
		return results, docids
	
	def _merge_parametric_indexes (self):
		'''
		Merge standard index with paramatric index if possible.
		'''
	
	def _create_scoring_data (self, postings_lists, query_tokens):
		'''
		::param postings_lists:: Postings lists return by the method _merge_indexes
		::return:: list of data, each element represent a document, and dependent on the rank object.
		'''
		return self.ranking.create_scoring_data (self, postings_lists, query_tokens)

	def _rank (self, docs=None, scores=None):
		'''
		Need to implement this one first. Possibly need to fix the merge to 
		include more information to rank the document being returned.
		'''
		results = []
		if docs is None or scores is None or len (docs) == 0 or len (scores) == 0:
			pass
		else:
			S_DOCID = self.ranking.SCORE_LISTS['DOCID']
			D_DOCID = 0
			docids = [d[S_DOCID] for d in scores]
			results = sorted (docs, key=lambda x: docids.index (x[D_DOCID]))
		return results

	def retrieve (self, query):
		'''
		The interface to combine all other method to retrieve the final results
		::param query:: a string of terms
		::return:: 
		'''
		tokens = self.preprocessing.run (query)[0]
		indexes = self._fetch_indexes (tokens)
		postings_lists, docids = self._merge_indexes (indexes)
		scoring_data = self._create_scoring_data (postings_lists, tokens)
		scores = self.ranking.score (scoring_data)
		docs = self.fetch_docs (docids)
		ranked_docs = self._rank (docs, scores)
		return ranked_docs