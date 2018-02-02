'''
Index construction.
'''
from collections import defaultdict
import pymongo

class Indexing:
	'''
	There are 3 way to represent indexes:
		a list of list [termid, [[], [], ...]]
		a dictionary {termid: [[], [], ...], ...}
		a list of dictionaries: [{termid: xxxx, pl: [[], ...]}, ...]
	First two are used in main memory and sometimes interact with each other. The choice depends on the purpose of an index. With cach indexes, most of the time, they are used for lookup only, store in dictionary helps speed up the performance. Other indexes like thosed result from _parse method, are of type list. They save space, and only used for the time of indexing.
	The last representation is used to store in disk.
	
	FUTURE WORK:
		+ Reconsider the index structures. Both structures being used may confusing sometimes.
		+ Need to deal with words being deleted from an indexed documents. The current version only deal with document being deleted, new words, an existing word being added or move arround a given document.
	'''

	def __init__ (self, db=None, preprocessing=None, max_update_termid=100):
		'''
		Functionality:
			+ Create and update indexes
			+ Create and update vocabulary

		::param index:: possibly not the whole index in the system, but only part of it, being cached for efficiency retrieval. If provided, help create new index or update index more efficiently.
		::param vocabulary:: cached by system and provided to help create new index or update index more efficiently. 
		''' 
		self.db = db
		self._vocabulary_coll = self.db.vocabulary_coll
		self._index_coll = self.db.index_coll
		self._doc_vector_coll = self.db.contentvectors_coll
		self.preprocessing = preprocessing
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
			'TERMID': 0,
			'POSTINGLIST': 1,
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
		self._reset_termid_queues ()

	def update_index_cache (self):
		'''
		Update indexes of terms that are most frequently looked up by users.
		'''
		pass

	def _create_vocabulary_cache (self):
		'''
		Fetch vocabulary from disk.
		By default, fetch and load all of vocabulary into main memory. 
		'''
		self._vocabulary = defaultdict (lambda: {'termid': None, 'df': 0, 'docid': None}, {})
		vocabulary = self._vocabulary_coll.find ()
		for v in vocabulary:
			self._vocabulary[v['term']] = {'termid': v['termid'], 'df': v['df'], 'docid': None}

	def _create_index_cache (self):
		'''
		Fetch most frequently used part of index from disk. << NEED TO IMPLEMENT >>
		'''
		self._indexes = defaultdict (lambda: None, {})	

	def _to_list_memory_indexes (self, indexes):
		'''
		Convert from disk indexes to memory indexes in form of a list of lists.
		'''	
		return [[i['termid'], i['pl']] for i in indexes]

	def _to_dict_memory_indexes (self, indexes):
		'''
		::param indexes:: could be a cursor or a list of disk indexes
		'''

		indexes = {i['termid']:i['pl'] for i in indexes}
		indexes = defaultdict (lambda: None, indexes)
		return indexes

	def _to_disk_indexes (self, indexes):
		'''
		Convert from list of list to list of dictionaries. 
		Used to convert indexes to proper form before save to disk.
		'''	
		TERMID = self.INDEX_LIST['TERMID']
		POSTINGLIST = self.INDEX_LIST['POSTINGLIST']
		return [{'termid': i[TERMID], 'pl': i[POSTINGLIST]} for i in indexes]

	def get_indexes (self):
		'''
		Get the cached indexes
		'''
		return self._indexes	

	def get_vocabulary (self):
		'''
		Get the cached vocabulary
		'''
		return self._vocabulary

	def save_vocabulary (self):
		'''
		Insert vocabulary to disk. 
		Most of the time, use update_vocabulary instead.
		'''
		if self._vocabulary is not None:
			vocabulary = [{'term': k, 'termid': v['termid'], 'df': v['df']} for k,v in self._vocabulary.items ()]
			self._vocabulary_coll.insert_many (vocabulary)

	def update_vocabulary (self):
		'''
		Update the document frequency of term
		'''
		if self._vocabulary is not None:
			new_termids = self._get_new_termids ()
			updated_termids = self._get_updated_termids ()
			if len (new_termids) > 0:
				vocabulary = [{'term': t, 'termid': v['termid'], 'df': v['df']} for t,v in self._vocabulary.items () if t in new_termids]
				self._vocabulary_coll.insert_many (vocabulary)
			if len (updated_termids) > 0:

				for k,v in self._vocabulary.items ():
					if k in updated_termids:
						self._vocabulary_coll.update_one ({'termid': v['termid']}, {'$set': {'df': v['df']}})
			self._reset_termid_queues ()

	def _new_termid (self, termid):
		'''
		Store a new termid discovered. For example when _parse is called.
		'''
		self._new_termid_queue.append (termid) 

	def _updated_termid (self, termid):
		'''
		Store updated termid in vocabulary. For example, when _parse is called, some termid have df increaseing.
		'''
		if termid not in self._new_termid_queue and termid not in self._updated_term_queue:
			self._updated_term_queue.append (termid)

	def _get_new_termids (self):
		return self._new_termid_queue
	
	def _get_updated_termids (self):
		return self._updated_term_queue

	def _reset_termid_queues (self): 
		self._new_termid_queue = []
		self._updated_term_queue = []

	def save_indexes (self, index=None, index_coll_name=None):
		'''
		Insert an index to disk. Most of the time use update_indexes instead
		'''	
		if index is None:
			raise ValueError ('Index or collection name must be provided.')
		index = self._to_disk_indexes (index)
		self._index_coll.insert_many (index)

	def update_indexes (self, indexes=None):
		'''
		Update postings lists of indexes in disk. Possibly, insert if not found termid.
		'''
		TERMID = self.INDEX_LIST['TERMID']
		PL = self.INDEX_LIST['POSTINGLIST']
		
		if indexes is None:
			raise ValueError ('Index must be provided.')

		termids = [i[TERMID] for i in indexes]
		updates = self._to_disk_indexes (indexes)
		self._index_coll.update_many (termids, updates)

	def fetch_indexes (self, termids=None):
		'''
		Fetch indexes from disk using their termids.
		::param termids:: a list of term IDs
		::return:: disk indexes.
		'''
		if termids is None:
			raise ValueError ('termid must be provided') 
		result = self._index_coll.find_many (termids)
		return result

	def get_doc_vectors (self, docids):
		'''
		Return the cursor of doc vectors.
		'''
		docvs = self._doc_vector_coll.find_many (docids)
		return docvs

	def _save_doc_vectors (self, doc_vectors):
		self._doc_vector_coll.insert_many (doc_vectors)

	def _update_doc_vectors (self, doc_vectors):
		'''
		Updated current doc vector. Possibly, insert if it is a new document.
		'''
		self._doc_vector_coll.update_many (doc_vectors)

	def _parse (self, tokens, docIDs):
		'''
		The return is a list of lists of a combination of termid, docid, and position index.
		The lists will be processd by another method to combine them into an index.
		Besides that, accumulate the vocabulary dictionary.
		<< NOT FIND the explanation make sense >> Normally tokens and docIDs should be store as attributes of the object. However, since the IR system does not parse the whole collection, but breake it in piece depending on the document status and process one group of collections at a time, the method is called several time to process a collection, and thus the choice of parameters will be more flexible.

		Beside parse, also build document vector, which record termid, term frequency, and docid.

		::param tokens::
		::type tokens::
		::param docIDs::
		::type docIDs::
		::return:: list of lists of a combination of termid, docid, and term index
		::rtype:: list of lists
		'''

		postings = []
		doc_vectors = []
		D = len (tokens)
		for i in range (D):
			doc = tokens [i]
			docid = docIDs[i]
			S = len (doc)
			pindex = 0 # position index
			docv_tf = defaultdict (lambda: 0, {})
			for j in range (S):
				sent = doc[j]
				for term in sent:
					V = len (self._vocabulary)
					if self._vocabulary[term]['termid'] is None: # new term
						self._vocabulary[term]['termid'] = V
						self._vocabulary[term]['docid'] = docid
						self._vocabulary[term]['df'] += 1
						self._new_termid (V)
					if self._vocabulary[term]['docid'] != docid: # not new term
						self._vocabulary[term]['df'] += 1
						self._vocabulary[term]['docid'] = docid
						self._updated_termid (self._vocabulary[term]['termid'])
					termid = self._vocabulary[term]['termid']
					postings.append ([termid, docid, pindex])
					docv_tf[termid] += 1
					pindex += 1
			docv_tf = [(k, v) for k,v in docv_tf.items ()]
			doc_vectors.append ({'docid': docid, 'tf': docv_tf})
		self._update_doc_vectors (doc_vectors)
		return postings

	def _index (self, postings):
		'''	
		Create indexes from postings.
		Sort the postings to make sure indexes of the same terms are placed next to each other. Doing so make it easier to create the indexes.
		For each term, combine position of the same term together to for the index of the term.
		FUTURE WORK: 
			- Keep the positions as a dictionary too, whose keys are the docID. 
			It helps to speed up the lookup, though it will take more space than using list.
	
		Two term `position` and `posting` are called interchargeably. Both refer 
		to the list [docid, tf, position1, position2, ...]		
		::param postings:: list of postings which obtained from the method _parse
		::return:: indexes whose format is 
			[[termid, [[docid, tf, position1, position2, ...], ....]], ...]
		'''

		P_TERMID = self.POSTING['TERMID']
		P_DOCID = self.POSTING['DOCID']
		P_TERM_POSISTION = self.POSTING['TERM_POSISTION']
		POSTINGLIST = self.INDEX_LIST['POSTINGLIST']
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
				positions_list = index[-1][POSTINGLIST]
				cur_docids = [i[PL_DOCID] for i in positions_list]
				if docid not in cur_docids:
					positions_list.append (positions)
				else:
					docid_index = cur_docids.index (docid)
					positions_list[docid_index].append (term_position)
					positions_list[docid_index][PL_TERM_FREQ] += 1
			cur_termid = termid
		return index

	def _index_new_docs (self, collection):
		'''
		Index new documents. 
		'''
		DOCID = self.COLLECTION['DOCID']
		DOC = self.COLLECTION['DOC']
		POSTINGLIST = self.INDEX_LIST['POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']
		docIDs = [d[DOCID] for d in collection]
		docs = [d[DOC] for d in collection]
		postings = self._parse (docs, docIDs)
		indexes = self._index (postings)

		for ti in indexes: # sort by docid
			postings_list = ti[POSTINGLIST]
			ti[POSTINGLIST] = sorted (postings_list, key=lambda x: x[PL_DOCID])
		
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

	def _left_join_postings_lists (self, pl, target_pl):
		'''
		Similar to left join in SQL in that the left side pl is keep intact in the result. However, the different is that in the target_pl, not the postings found, but not found are the ones being added to the result. 
		The two postings lists are joined by docIDs.

		Assume both pl and target_pl are not empty.
		::return:: pl after join with target.
		'''	

		POSTINGLIST = self.INDEX_LIST['POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']
		target_docids = [p[PL_DOCID] for p in target_pl]
		docids = [p[PL_DOCID] for p in pl]
		matched_i = [target_docids.index (d) for d in target_docids if d in docids]
		not_matched = [p for p in target_pl if target_pl.index (p) not in matched_i]
		pl = pl + not_matched
		pl = sorted (pl, key=lambda x: x[PL_DOCID])
		return pl
				
	def _merge_indexes (self, new_indexes, edited_indexes):
		'''
		Merge edited_indexes, new_indexes, cache indexes (if any), and disk indexes (if any).
		ALGORITHM:
			+ Merge edited indexes with cached indexes if any
			+ Obtain disk indexes using termids of unmerged indexes, and then merged with the unmerged of edited indexes.
			+ if still existed some unmerged, they are new term, and thus just they are in the edited indexes (or merged indexes) and do not need to do anything.
			+ Merged two parts of the edited indexes above, by a simple adding operation between two list. It is so simple since it is guaranteed that two list are not overlapped in term of termid.
			+ Merge the result above with new indexes. If a term has existed, just append its postings to the end of the posting list in the merged indexes, since new index has total new docid.
			+ Sort according to docid. 
		FUTURE WORK:
			+ Keep termid of updated cached indexes, and after indexing, update the cached indexes.

		::return:: merged indexes which include indexes from all aboved indexes.
		'''


		TERMID = self.INDEX_LIST['TERMID']
		POSTINGLIST = self.INDEX_LIST['POSTINGLIST']
		PL_DOCID = self.INDEX_LIST['PL_DOCID']

		# merge edited_indexes with cache_indexes
		unmerged_edited_indexes = []
		unmerged_edited_i = []
		if len (self._indexes) > 0: # merge with cache indexes
			L = len (edited_indexes)
			for i in range (L):
				ti = edited_indexes[i]
				termid = ti[TERMID]
				pl = ti[POSTINGLIST]
				cache_pl = self._indexes[termid]
				if cache_pl is not None:
					ti[POSTINGLIST] = self._left_join_postings_lists (pl, cache_pl)
				else:
					unmerged_edited_indexes.append (ti)
					unmerged_edited_i.append (i)

		# merge edited_indexes with disk_indexes
		unmerged_termids = [ti[TERMID] for ti in unmerged_edited_indexes]
		disk_indexes = self.fetch_indexes (unmerged_termids)
		disk_indexes = self._to_dict_memory_indexes (disk_indexes)
		if len (disk_indexes) > 0: # merge with disk indexes
			for ti in unmerged_edited_indexes:
				termid = ti[TERMID]
				pl = ti[POSTINGLIST]
				disk_pl = disk_indexes.get (termid, None)
				if disk_pl is not None:
					ti[POSTINGLIST] = self._left_join_postings_lists (pl, disk_pl)
		
		# replace updated indexes into the edited_indexes
		for i,k in zip (unmerged_edited_i, unmerged_edited_indexes):
			edited_indexes[i] = k

		# merge edited_indexes with new_indexes
		ei_termids = [i[TERMID] for i in edited_indexes]
		ei_termid_i = [edited_indexes.index (i) for i in edited_indexes]
		ni_termids = [i[TERMID] for i in new_indexes]
		ni_termid_i = [new_indexes.index (i) for i in new_indexes]
		new_termid_indexes = [new_indexes[i] for i,j in zip (ni_termid_i,ni_termids) if j not in ei_termids]
		exist_termids = [i for i in ni_termids if i in ei_termids]
		for t in exist_termids: 
			ni = ni_termids.index (t)
			ei = ei_termids.index (t)
			edited_indexes[ei][POSTINGLIST].extend (new_indexes[ni][POSTINGLIST])
		edited_indexes.extend (new_termid_indexes)
		return edited_indexes

	def index (self, collection=None, save=True):
		'''
		Most external use of the class is carried through the method. 
		Create or update indexes.
		Assume that no document is duplicated, which means no two document have the same docid.
		
		::param collection:: a list of [docid, doc states, doc content]. If not being preprocessing, must provide preprocessing when init the object.
		::param save:: True means to build and save index to disk. Otherwise, just return the index without saving to disk.
		'''
		if not collection:
			return None

		STATE = self.COLLECTION['STATE']
		DOC = self.COLLECTION['DOC']
		DOCID = self.COLLECTION['DOCID']		
		NEW_STATE = self.DOC_STATES ['NEW']
		EDIT_STATE = self.DOC_STATES ['EDITED']
		DELETE_STATE = self.DOC_STATES ['DELETED']

		collection = [[d[DOCID], self.preprocessing.run (d[DOC]), d[STATE]] for d in collection]
		new_docs = [d for d in collection if d[STATE] == NEW_STATE]
		edited_docs = [d for d in collection if d[STATE] == EDIT_STATE]
		deleted_docs = [d for d in collection if d[STATE] == DELETE_STATE]
		new_indexes = self._index_new_docs (new_docs)
		edited_indexes = self._index_edited_docs (edited_docs)
		self._unindex_deleted_docs (deleted_docs)
		merged_indexes = self._merge_indexes (new_indexes, edited_indexes)
		self.update_indexes (merged_indexes)
		self.update_vocabulary ()
		self.update_index_cache ()
		return True
		
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
		self._index_coll = self.db['bsb_indexes']

	def _insert_block (self, index, blockid):
		'''
		Insert a block to disk.
		::param index::
		::param blockid:: index of the current block
		'''
		TERMID = 0
		PL = 1	
		index_coll_name = self.temp_index_coll_template.format (blockid)
		_index_coll = self.db[index_coll_name]
		inserted_index = [{'termid': i[TERMID], 'pl': i[PL:]} for i in index]
		_index_coll.insert_many (inserted_index)		

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
		Just break the collection into small pieces and call Indexing.index to process it.
		No longer use merge_block, since Indexing.index do all the work.
		::return::
		::rtype::
		::param collection:: a preprocessed collection of tokens, whose format is [[docid, doc], ...]
		::type collection:: a file generator
		::param block_size:: max size of a block of documents
		::type block_size:: an integer
		'''

class SPIMIndexing (Indexing): pass

def bsbi_demo ():
	collection = []
	
def spimi_demo (): pass

if __name__ == '__main__': pass