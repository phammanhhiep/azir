import os,sys
sys.path.insert (0, os.path.abspath ('./'))
import pymongo

class Retrieval:
	'''
	Retrieve and rank results before return.
	Access indexes and vocabulary through indexing instance. This makes sense since indexing cache the whole vocabulary and part of the indexes.
	
	Assume web search. In other words, assume each term in a query is joined with another by an AND operator.
	
	::param indexing:: an instance of class Indexing.
	::param rank:: an instance of class Rank
	::param preprocessing:: an instance of class Preprocessing
	'''

	def __init__ (self, dbname='market',rank=None, preprocessing=None, indexing=None):
		self.db = pymongo.MongoClient ()[dbname]
		self.preprocessing = preprocessing
		self.indexing = indexing
		self.rank = rank
		self._create_cache ()
		self._setup_contants ()

	def _setup_contants (sefl): pass
	
	def _create_cache (self): pass	

	def _fetch_postings_lists (self, tokens):
		'''
		Get the indexes of the tokens
		'''
		postings_lists = []
		fetched_termids = []
		tokens = list (set (tokens))
		vocabulary = self.indexing.get_vocabulary ()
		indexes = self.indexing.get_indexes ()
		termids = [vocabulary[t]['termid'] for t in tokens]
		index_len = len (indexes)
		
		if index_len > 0:
			for t in termids:
				temp_pl = indexes[t]
				if temp_pl is not None:
					postings_lists.append (temp_pl)
				else:
					fetched_termids.append (t)
		else:
			fetched_termids = termids
		
		indexes = self.indexing.fetch_indexes (fetched_termids)
		for termid, pl in indexes.items ():
			postings_lists.append (pl)
		
		postings_lists = sorted (postings_lists, key=lambda x: len (x))
		
		return postings_lists

	def _merge_postings_lists (self, postings_lists, k=100):
		'''
		Assume the postings lists are sorted according to doc frequency.
		Group the term positions into lists corresponding to each document.
		Sort term postions according to the term positions values.
		
		For each of new position lists, process from left to right (low postion to high postion), obtain a lists of postions that meet the below requirements:
			- Two consecutive term postions must be close to each other within k distance
			- No term has two instances being place next to each other without any instance of other term being placed between them. <<< NOTICE: The condition should be used only if the system cannot tokenize words well enough, since it is possible to condition part of a term as an individual term and return misleading results. For example like "New York Times", with poor word tokenize tool, each is treated as individual word and without the condition, the relevance of results will be low. Once the confidence in word tokenization is high, should not enforce the condition. >>>
		
		The result posting lists are ranked in descending order according to how many query token each document contains.	
		
		FUTURE WORK:
		- Consider to enfore another condition: the order of between the positions of two terms match with order of corresponding query terms. However, a user may enter keywords rather than a sentence, and thus the order may not so important. 

		::param postings lists:: a list of postings in the same form created by instance of class Indexing
		::param k:: Max number of distanace between consecuitive term in query. Default is very large distance to ensure that such requirement is not enfored. 
		::return:: postings lists that contain at least one of the terms in query and being sorted according to the number of term they contain.
		'''	
		docs = defaultdict (lambda: [], {})
		merged_pl = []
		plnum = len (postings_lists)
		if plnum == 0: pass # no keywords found
		elif plnum == 1: # only one keywords
			merged_pl = [[d[0], d[1:], 0] for d in postings_lists[0]['did']]
		else: # more than one keywords found
			merged_docids = None
			for i in range (plnum-1):
				curpl = postings_lists[i]
				nextpl = postings_lists[i+1]
				curdocs = set ([d[0] for d in curpl['did']]) if merged_docids is None else set(merged_docids)
				nextdocs = set ([d[0] for d in nextpl['did']])
				merged_docids = list (curdocs.intersection (nextdocs))
			
			if len (merged_docids) == 0: # no doc contains all the terms
				merged_pl = []
			else: # the two terms occur in the some common docs
				for pl in postings_lists: # remove docs that do not contain all terms
					pl['did'] = [l for l in pl['did'] if l[0] in merged_docids]

				for pl in postings_lists: # group termids according to docid
					did_list = pl['did']
					termid = pl['termid']
					for d in did_list:
						for l in d[1:]:
							docs[d[0]].append ((termid, l))
				
				TERMID = 0; POSITION = 1;
				for did, term_indexes in docs.items (): # keep term postions if meet requirements
					term_indexes = sorted (term_indexes, key=lambda x: x[POSITION])
					tnum = len (term_indexes)
					selected_indexes = []
					for i in range (tnum-1):
						cur_term = term_indexes[i]
						next_term = term_indexes[i + 1]
						offset = next_term[POSITION] - cur_term[POSITION]
						if cur_term[TERMID] == next_term[TERMID]: continue 
						if offset <= k and offset > 0:
							selected_indexes.extend ([cur_term, next_term])
					if len (selected_indexes) > 0:
						selected_indexes = list (set (selected_indexes))

					selected_termids = [i[TERMID] for i in selected_indexes]
					selected_tnum = len (list (set (selected_termids)))
					selected_positions = sorted ([i[POSITION] for i in selected_indexes])
					merged_pl.append ([did, selected_positions, selected_tnum])
		if len (merged_pl):
			merged_pl = sorted (merged_pl, key=lambda x: x[2], reverse=True)
		return merged_pl
	
	def _merge_postings_lists_with_parametric_indexes (self):
		'''
		Merge standard index with paramatric index if possible.
		'''

	def _fetch_docs (self, postings_lists): pass

	def _rank (self, docs):
		'''
		Need to implement this one first. Possibly need to fix the merge to include more information to rank the document being returned.
		'''

	def retrieve (self, query):
		'''
		The interface to combine all other method to retrieve the final results
		'''
		tokens = self.preprocessing.run ([query])[0][0]
		postings_lists = self._fetch_postings_lists (tokens)
		merged_postings_lists = self._merge_postings_lists (postings_lists)
		docs = self._fetch_docs (merged_postings_lists)
		docs = self._rank (docs)
		return docs

