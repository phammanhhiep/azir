import os,sys
sys.path.insert (0,os.path.abspath ('./app_ir/'))

from indexing.indexing import BSBIndexing as Indexing
from preprocess.preprocess import Preprocessing
from retrieve.retrieval import Retrieval

class IRSYS:
	def __init__ (self, dbname='market', doc_dbname='market', doc_coll_name='contents', vocabulary_coll_name='vocabularies', index_coll_name='indexes', max_queue=100, max_delete_cache=100, max_update_wait_time=300, max_delete_wait_time=300):
		'''
		Set up parameters and update index from the last queue and cache.

		::param max_queue:: max number of docids in the queue
		::param max_delete_cache:: max number of docids in the cache
		::param max_update_wait_time:: max waiting time in seconds before update documents with docids in queue
		::param max_delete_wait_time:: max waiting time in seconds before delete documents with docids in the delete cache.
		'''
		self.max_queue = max_queue
		self.max_delete_cache = max_delete_cache
		self.max_update_wait_time = max_update_wait_time
		self.max_delete_wait_time = max_delete_wait_time
		self.dbname = dbname
		self.doc_coll_name = doc_coll_name
		self._create_queue ()
		self._create_cache ()
		self.indexing = Indexing (dbname=dbname, vocabulary_coll_name=vocabulary_coll_name, index_coll_name=index_coll_name)
		self.retrieval = Retrieval ()
		self.index ()

	def _create_cache (self):
		'''
		Cache:
			+ vocabulary and most frequently used index.
			+ delete cache
		'''	
		self._create_delete_cache ()

	def _create_delete_cache (self): 
		'''
		Used to keep docids of documents being deleted by users.  
		'''

	def _create_queue (self):
		'''
		Used to store docids and their states in the queue
		'''
		self._queue = []

	def get_queue (self):
		return self._queue

	def check_queue (self, doc=None):
		'''
		Check conditions to process docid in the queues.
		Filter the queue if the doc has one instance in the queue and update appropriate state. For example, it was created but not being indexed yet, and now it is edited. In another case, it is created but not indexed, but now being deleted. First case will keep the edited version, but now the document is considered as a new document since it has nevered been index. The later require just ignore the document.
		
		::param doc:: the new doc about being indexed. 
		::return:: True if ready to add or else False
		'''
	def reset_queue (self):
		'''
		Reset queue and its states
		'''
	
	def docid_queue_add (self):
		'''
		Add docid and its state to the queue
		'''

	def _fetch_doc_content (self, docs):
		'''
		Get document content, add the content to correspind doc object.
		Sort document in order of created time (or docID which is assumed to be an ObjectId)
		::param docs:: a dictionary, whose key is a docid, and value is its state 
		::return:: a list of list each of which has format, [docid, state, content]
		'''

	def index (self, doc=None):
		'''
		Add to queue if possible or to index all documents whose docids in the queue.

		::param docs:: a dictionary, whose key is a docid, and value is its state 
		'''
		result = None
		if doc is None:
			old_docs = self.get_queue ()
			old_docs = self._fetch_doc_content (old_docs)
			self.reset_queue ()
			result = self.indexing.index (old_docs)
		else:
			if self.check_queue (doc):
				self.queue_add (doc)
			else:
				old_docs = self._get_queue ()
				old_docs = self._get_doc_content (old_docs)
				self.reset_queue ()
				self.queue_add (doc)
				result = self.indexing.index (old_docs)
		return result

	def retrieve (self, query):
		result = self.retrieval.retrieve (query)
		return result
