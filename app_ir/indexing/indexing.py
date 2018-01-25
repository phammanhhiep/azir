'''
Index construction.
'''

import os,sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.compress.compression import Compression
from app_ir.preprocess.preprocess import Preprocessing
from app_ir.preprocess.stem import Stem
from app_ir.preprocess.word_tokenize import WordTokenize
from app_ir.preprocess.sent_tokenize import SentTokenize
from app_ir.preprocess.spell_correct import SpellCorrection

from collections import defaultdict
import pymongo

class Indexing:
	def __init__ (self, dbname='market', vocabulary_coll_name='vocabularies', index_coll_name='indexes', max_update_termid=100):
		'''
		Functionality:
			+ Create and update indexes
			+ Create and update vocabulary

		::param index:: possibly not the whole index in the system, but only part of it, being cached for efficiency retrieval. If provided, help create new index or update index more efficiently.
		::param vocabulary:: cached by system and provided to help create new index or update index more efficiently. 
		''' 
		self.db = pymongo.MongoClient ()[dbname]
		self.vocabulary_coll = self.db[vocabulary_coll_name]
		self.index_coll = self.db[index_coll_name]
		self.max_update_termid = max_update_termid
		self.create_cache ()
		self.setup_constants ()

	def setup_constants (self):
		self.COLLECTION = {
			'DOCID': 0,
			'DOC': 1,
			'STATE': 2,
		}

		self.INDEX_LIST = {
			'I_TERMID': 0,
			'I_POSTINGLIST': 1,
			'PL_DOCID': 0,
			'PL_TERM_FREQ': 1,
			'PL_TERM_POSITION': 2,
		}

		self.POSTING = {
			'TERMID': 0,
			'DOCID': 1,
			'TERM_POSISTION': 2		
		}

		self.DOC_STATES = {
			'NEW': 0,
			'EDITED': 1,
			'DELETED': 2
		}

	def create_cache (self):
		'''
		::variable _index_cache_update_queue:: list of termid. Used to store termids of indexes that should be updated 

		'''
		self._create_vocabulary_cache ()
		self._create_index_cache ()
		self._update_termid_queue = []

	def update_index_cache (self):
		'''
		Update index of terms that are most frequently looked up by users.
		'''
		if len (self._update_termid_queue) > self.max_update_termid:
			pass

	def _create_vocabulary_cache (self):
		'''
		Fetch vocabulary from disk.
		By default, fetch and load all of vocabulary into main memory. 
		'''
		self._vocabulary = defaultdict (lambda: {'termid': None, 'df': 0, 'docid': None}, {})
		vocabulary = self.vocabulary_coll.find ()
		for v in vocabulary:
			self._vocabulary[v['term']] = {'termid': v['termid'], 'df': v['df']}

	def _create_index_cache (self):
		'''
		Fetch most frequently used part of index from disk.
		'''
		self._indexes = defaultdict (lambda: None, {})	

	def get_indexes (self):
		return self._indexes	

	def get_vocabulary (self):
		return self._vocabulary

	def _parse (self, tokens, docIDs):
		'''
		The return is a list of lists of a combination of termid, docid, and position index.
		The lists will be processd by another method to combine them into an index.
		Besides that, accumulate the vocabulary dictionary.
		Normally tokens and docIDs should be store as attributes of the object. However, since the IR system does not parse the whole collection, but breake it in piece and process one at a time, the method is called several time to process a collection, and thus the choice of parameters will be more flexible.

		::param tokens::
		::type tokens::
		::param docIDs::
		::type docIDs::
		::return:: list of lists of a combination of termid, docid, and term index
		::rtype:: list of lists
		'''
		postings = []
		D = len (tokens)
		for i in range (D):
			doc = tokens [i]
			docid = docIDs[i]
			S = len (doc)
			pindex = 0 # position index
			for j in range (S):
				sent = doc[j]
				for term in sent:
					V = len (self._vocabulary)
					if self._vocabulary[term]['termid'] is None:
						self._vocabulary[term]['termid'] = V
					if self._vocabulary[term]['docid'] != docid:
						self._vocabulary[term]['df'] += 1
						self._vocabulary[term]['docid'] = docid
					termid = self._vocabulary[term]['termid']
					postings.append ([termid, docid, pindex])
					pindex += 1
		return postings

	def _index (self, postings):
		'''	
		Create indexes from postings.
		Sort the postings to make sure indexes of the same terms are placed next to each other. Doing so make it easier to create the indexes.
		For each term, combine position of the same term together to for the index of the term.
		FUTURE WORK: 
			- Keep the positions as a dictionary too, whose keys are the docID. It helps to speed up the lookup, though it will take more space than using list.
	
		Two term `position` and `posting` are called interchargeably. Both refer to the list [docid, tf, position1, position2, ...]		
		::param postings:: list of postings which obtained from the method _parse
		::return:: an index
		'''

		P_TERMID = self.POSTING['TERMID']
		P_DOCID = self.POSTING['DOCID']
		P_TERM_POSISTION = self.POSTING['TERM_POSISTION']
		I_POSTINGLIST = self.INDEX_LIST['I_POSTINGLIST']
		PL_TERM_FREQ = self.INDEX_LIST['PL_TERM_FREQ']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']
		index = []
		postings = sorted (postings, key=lambda x: x[P_TERMID])
		cur_termid = None
		for p in postings:
			termid = p[P_TERMID]
			docid = p[P_DOCID]
			term_position = p[P_TERM_POSISTION]
			init_tf = 1
			positions = [docid, init_tf, term_position] 
			if cur_termid != termid:
				index.append ([termid, [positions]])
			elif cur_termid == termid:
				positions_list = index[-1][I_POSTINGLIST]
				cur_docids = [i[PL_DOCID] for i in positions_list]
				if docid not in cur_docids:
					positions_list.append (positions)
				else:
					docid_index = cur_docids.index (docid)
					positions_list[docid_index].append (term_position)
					positions_list[docid_index][PL_TERM_FREQ] += 1
			cur_termid = termid
		return index

	def save_vocabulary (self):
		'''
		Insert vocabulary to disk.
		'''
		if self._vocabulary is not None:
			vocabulary = [{'term': k, 'termid': v['termid'], 'df': v['df']} for k,v in self._vocabulary.items ()]
			self.vocabulary_coll.insert_many (vocabulary)

	def update_vocabulary (self, data=None):
		'''
		Update the document frequency of term
		::param data:: a part of the vocabulary to update. Each is a dictionary whose keys are ['termid', 'df']
		'''
		if self._vocabulary is not None and data is not None:
			for d in data:
				self.vocabulary_coll.update_one ({'termid': d['termid']}, {'$inc': {'df': d['df']}})

	def save_indexes (self, index=None, index_coll_name=None):
		'''
		Insert an index to disk.
		'''	
		if index is None:
			raise ValueError ('Index or collection name must be provided.')
		if index_coll_name is not None:
			self.db[index_coll_name].insert_many (index)
		else:
			self.index_coll.insert_many (index)

	def update_indexes (self, index=None, index_coll_name=None):
		'''
		Update postings lists of indexes in disk.
		'''
		if index is None:
			raise ValueError ('Index must be provided.')
		if index_coll_name is not None:
			index_coll = self.db[index_coll_name]
		else:
			index_coll = self.index_coll
		for i in index:
			termid = i['termid']
			pl = i['pl']
			index_coll.update_one ({'termid': termid}, {'$set': {'termid': termid, 'pl': pl}}, upsert=True)

	def fetch_indexes (self, termids=None):
		'''
		Fetch indexes from disk using their termids.
		::param termids:: a list of term IDs
		'''
		if termids is None:
			raise ValueError ('termid must be provided') 
		result = self.index_coll.find ({'termid': {'$in': termids}})
		indexes = {}
		for i in result:
			indexes[i['termid']] = i['pl']
		return indexes

	def _index_new_docs (self, collection):
		'''
		REVIEW this one. Should not merge with disk and cache indexes right away. Better combine with edited indexes first. After that merge them with disk and cache indexes.  
		'''
		DOCID = self.COLLECTION['DOCID']
		DOC = self.COLLECTION['DOC']
		I_TERMID = self.INDEX_LIST['I_TERMID']
		I_POSTINGLIST = self.INDEX_LIST['I_POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']
		docIDs = [d[DOCID] for d in collection]
		docs = [d[DOC] for d in collection]
		postings = self._parse (docs, docIDs)
		indexes = self._index (postings)

		for ti in indexes: # sort by docid
			postings_list = ti[I_POSTINGLIST]
			ti[I_POSTINGLIST] = sorted (postings_list, key=lambda x: x[PL_DOCID])
		
		return indexes		

	def _index_edited_docs (self, collection):
		'''
		Adopt the simple approach: treat edited document as a new document. 
		FUTURE WORK:
			- Determine how much documents are edited, and having separate solutions for different degrees in which they are edited
		'''
		return self._index_new_docs (collection)		
	
	def _unindex_deleted_docs (self, collection):
		'''
		Remove docids of deleted documents from the indexes in disk and update cache index.
		::param collection:: docids of the documents.
		'''

	def _inverse_left_join_postings_lists (self, pl, target_pl):
		'''
		Similar to left join in SQL in that the left side pl is keep intact in the result. However, the different is that in the target_pl, not the postings found, but not found are the ones being added to the result. 
		The two postings lists are joined by docIDs.
		::return:: pl after join with target.
		'''	

		I_POSTINGLIST = self.INDEX_LIST['I_POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']

		target_docids = [p[PL_DOCID] for p in target_pl]
		docids = [p[PL_DOCID] for p in pl]
		matched_i = [target_docids.index (d) for d in target_docids if d in docids]
		not_matched_target_pl = [p for p in target_pl if target_pl.index (p) not in matched_i]
		pl = pl + not_matched_cache_pl
		pl = sorted (pl, key=lambda x: x[PL_DOCID])
		return pl
				
	def _union_posting_lists (self, pl, target_pl):
		'''
		Add each postings in the target_pl to the pl.
		'''
		pl.extend (target_pl)
		return pl

	def _merge_indexes (self, new_indexes, edited_indexes):
		'''
		Merge edited_indexes, new_indexes, cache indexes (if any), and disk indexes (if any).
		The process:
			+ Merge edited indexes with cached indexes if any
			+ Obtain disk indexes using termids of unmerged indexes, and then merged with the unmerged of edited indexes.
			+ if still existed some unmerged, they are new term, and thus just they are in the edited indexes (or merged indexes) and do not need to do anything.
			+ Merged two parts of the edited indexes above, by a simple adding operation between two list. It is so simple since it is guaranteed that two list are not overlapped in term of termid.
			+ Merge the result above with new indexes. If a term has existed, just append its postings to the end of the posting list in the merged indexes, since new index has total new docid.
			+ Sort according to docid. 

		::return:: merged indexes which include indexes from all aboved indexes.
		'''

		I_TERMID = self.INDEX_LIST['I_TERMID']
		I_POSTINGLIST = self.INDEX_LIST['I_POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']

		# merge edited_indexes with cache_indexes
		unmerged_edited_indexes = []
		unmerged_edited_i = []
		if len (self._indexes) > 0: # merge with cache indexes
			L = len (edited_indexes)
			for i in range (L):
				ti = edited_indexes[i]
				termid = ti[I_TERMID]
				pl = ti[I_POSTINGLIST]
				cache_pl = self._indexes[termid]
				if cache_pl is not None:
					ti[I_POSTINGLIST] = self._inverse_left_join_postings_lists (pl, cache_pl)
					self._update_termid_queue.append (termid)
				else:
					unmerged_edited_indexes.append (ti)
					unmerged_edited_i.append (i)

		# merge edited_indexes with disk_indexes
		unmerged_termids = [ti[I_TERMID] for ti in unmerged_edited_indexes]
		disk_indexes = self.fetch_indexes (unmerged_termids)
		if len (disk_indexes) > 0: # merge with disk indexes
			for ti in unmerged_edited_indexes:
				termid = ti[I_TERMID]
				pl = ti[I_POSTINGLIST]
				disk_pl = disk_indexes.get (termid, None)
				if disk_pl is not None:
					ti[I_POSTINGLIST] = self._merge_postings_lists (pl, cache_pl)
		
		# replace updated indexes into the edited_indexes
		for i in unmerged_edited_i:
			edited_indexes[i] = unmerged_edited_indexes[i]

		# merge edited_indexes with new_indexes
		edited_termids = [i[I_TERMID] for i in new_indexes]
		new_termids = [i[I_TERMID] for i in new_indexes]
		existing_term_i = [(edited_termids.index (i),new_termids.index (i)) for i in new_termids if i in edited_termids]
		new_term_indexes = [new_indexes[i] for i in new_termids if i not in edited_termids]
		for ei, ni in existing_term_i: 
			epl = edited_indexes[ei][I_POSTINGLIST]
			npl = new_indexes[ni][I_POSTINGLIST]
			edited_indexes[ei][I_POSTINGLIST] = self._union_posting_lists (epl, npl)
		edited_termids.extend (new_term_indexes)

		return edited_termids

	def index (self, collection, save=True):
		'''
		Most external use of the class is carried through the method. 
		Create or update indexes.
		Assume that no document is duplicated, which means no two document have the same docid.

		::param collection:: a list of [docid, doc states, doc content]
		::param save:: True means to build and save index to disk. Otherwise, just return the index without saving to disk.
		'''
		NEW_STATE = self.DOC_STATES ['NEW']
		EDIT_STATE = self.DOC_STATES ['EDITED']
		DELETE_STATE = self.DOC_STATES ['DELETED']
		new_docs = [d for d in collection if d[1] == NEW_STATE]
		edited_docs = [d for d in collection if d[1] == EDIT_STATE]
		deleted_docs = [d for d in collection if d[1] == DELETE_STATE]
		new_indexes = self._index_new_docs (new_docs)
		edited_indexes = self._index_edited_docs (new_docs)
		self._unindex_deleted_docs (deleted_docs)
		merged_indexes = self._merge_indexes (self, new_indexes, edited_indexes)
		self.update_indexes (merged_indexes)
		self.update_index_cache ()
		

class BSBIndexing(Indexing):
	'''
	An index can be store in a dictionary or a list, depending on their usage.
	self._indexes is the cache, which is used for quick lookup for a termid, and thus it is implement as a dictionary. However, an index of a document, which eventually is insert to disk is implemented as a list since there is no need for lookup and doing so keep its size smaller.

	No longer use the scheme of merging blocks. The parent class Indexing has class index, which already merged and updated to the indexes in disk. Overlapping method `index` of the class only break the document into peice and let its parent's `index` do the rest.

	Keep `insert_block` and `_merge_blocks` for reference later. Not use them.
	'''

	def __init__ (self, dbname='market', vocabulary_coll_name='vocabularies', index_coll_name='indexes', max_update_termid=100):
		Indexing.__init__ (self, dbname, vocabulary_coll_name, index_coll_name, max_update_termid)
		self.temp_index_coll_template = 'temp_bsb_indexes{}'
		self.index_coll = self.db['bsb_indexes']

	def _insert_block (self, index, blockid):
		'''
		Insert a block to disk.
		::param index::
		::param blockid:: index of the current block
		'''
		TERMID = 0
		PL = 1	
		index_coll_name = self.temp_index_coll_template.format (blockid)
		index_coll = self.db[index_coll_name]
		inserted_index = [{'termid': i[TERMID], 'pl': i[PL:]} for i in index]
		index_coll.insert_many (inserted_index)		

	def _get_block (self, blockname):
		'''
		::return:: sorted list of index according to its terms
		'''
		return list (self.db[blockname].find ().sort ('termid', 1))

	def _merge_blocks (self, dnum, block_size):
		''' 
		Combine all blocks into one block. 
		Each block could possibly contains the same terms, but always different documents for each term. The requirement is ensured in parsing and indexing. 
		After each successful merge, drop the being merged block, and keep the merging block in memory for the next iteration.

		FUTURE WORK:
			- Why need to break into block and later merged? Read again the IR textbook. 
			- Should keep the merged block in memory like I did? 
			- Consider to add `skip pointer` when merge two blocks, using term ids.
		'''
		merged_index = None
		first_block_name = self.temp_index_coll_template.format (0)
		for i in range (0, dnum - block_size, block_size):
			if merged_index is None:
				merged_index = self._get_block (first_block_name)
				self.db[first_block_name].drop ()
			next_block_name = self.temp_index_coll_template.format (i + block_size)
			next_index = self._get_block (next_block_name)
			temp_index = [j for j in merged_index]
			cur_num = len (merged_index)
			next_num = len (next_index)
			cur_pointer = 0
			next_pointer = 0
			while next_pointer < next_num:
				if cur_pointer < cur_num:
					cur_i = merged_index[cur_pointer]
					next_i = next_index[next_pointer]
					if cur_i['termid'] == next_i['termid']:
						cur_i['pl'].extend (next_i['pl'])
						cur_pointer += 1
						next_pointer += 1							
					elif cur_i['termid'] > next_i['termid']:
						temp_index.append (next_i)
						next_pointer += 1
					else:
						cur_pointer += 1
				elif cur_pointer == cur_num:
					rest = next_index[next_pointer:]
					temp_index.extend (rest)
					next_pointer = next_num			
			self.db[next_block_name].drop ()
			merged_index = temp_index
		
		self.save_indexes (merged_index)
		return merged_index

	def index (self, collection, block_size=500, **kwargs):
		'''
		FIX

		::return::
		::rtype::
		::param collection:: a preprocessed collection of tokens, whose format is [[docid, doc], ...]
		::type collection:: a file generator
		::param block_size:: max size of a block of documents
		::type block_size:: an integer
		'''
		if collection is None:
			raise ValueError ('collection must be provided.')
		D = len (collection)
		if D > block_size:			
			for i in range (0, D, block_size):
				block = collection[i:i+block_size]
				index = self.update_index (block)
				self._insert_block (index, i)
			self._merge_blocks (D, block_size)
		else:
			index = self.update_index (collection)
			self.update_indexes (index) 
		self.save_vocabulary ()		

class SPIMIndexing (Indexing): pass

def bsbi_demo ():
	collection = []
	tokens = Preprocessing.preprocess (collection, Stem=Stem, SpellCorrection=SpellCorrection, SentTokenize=SentTokenize, WordTokenize=WordTokenize)

def spimi_demo (): pass

if __name__ == '__main__': pass