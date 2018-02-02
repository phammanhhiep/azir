from app_ir.indexing.indexing import Indexing
from app_ir.preprocess.preprocess import Preprocessing
from app_ir.retrieve.retrieval import Retrieval
from app_ir.retrieve.ranking import CosineScoring

class IRSYS:
	def __init__ (self, db=None, dbname='market', max_queue=100, max_delete_cache=100, max_update_wait_time=300, max_delete_wait_time=300):
		'''
		Set up parameters and index documents from the last queue and cache.

		::param max_queue:: max number of docids in the queue
		::param max_delete_cache:: max number of docids in the cache
		::param max_update_wait_time:: max waiting time in seconds before update documents with docids in queue
		::param max_delete_wait_time:: max waiting time in seconds before delete documents with docids in the delete cache.
		'''
		self.max_queue = max_queue
		self.max_delete_cache = max_delete_cache
		self.max_update_wait_time = max_update_wait_time
		self.max_delete_wait_time = max_delete_wait_time
		self.db = db
		self._create_cache ()
		self._ranking = CosineScoring ()
		self._preprocessing = Preprocessing () 		
		self._indexing = Indexing (db=db, preprocessing=self._preprocessing) 
		self._retrieval = Retrieval (db=db, preprocessing=self._preprocessing, ranking=self._ranking, indexing=self._indexing)
		self.index ()

	def _create_cache (self):
		'''
		Cache:
			+ vocabulary and most frequently used index.
			+ delete cache
		'''	
		self._create_delete_cache ()
		self._create_queue ()

	def _create_delete_cache (self): 
		'''
		Used to keep docids of documents being deleted by users.  
		'''

	def _create_queue (self):
		'''
		Used to store docids and their states in the queue
		'''
		self._queue = []

	def _get_queue (self):
		return self._queue

	def _reset_queue (self):
		'''
		Reset queue and its states
		'''
		self._create_queue()
	
	def _queue_add (self, doc):
		'''
		Add docid and its state to the queue
		'''
		status = False
		if len (self._queue) < self.max_queue:
			self._queue.append (doc)
			status = True
		return status

	def _drop_queue (self):
		'''
		Delete queue in disk once all docs in queue are processed.
		'''	
		self.db.queued_content_coll.drop ()

	def save_queue (self):
		'''
		In some situations, a queue needs to be saved like 
		when the system is down suddently.
		'''	
		self.db.queued_content_coll.insert_many (self._queue)

	def _fetch_doc_content (self, docs=None):
		'''
		Get document content, add the content to correspind doc object.
		::param docs:: a dictionary, whose key is a docid, and value is its state 
		::return:: a list of list each of which has format, [docid, content, state]
		'''
		DOCID = 0
		STATE = 1
		F_DOCID = 0
		F_CONTENT = 1
		F_STATE = 2
		if docs is None:
			raise ValueError ('No documents are provided.')

		docids = [d[DOCID] for d in docs]
		states = [d[STATE] for d in docs]
		foundDocs = self._retrieval.fetch_docs (docids)
		foundDocs = sorted (foundDocs, key=lambda x: docids.index (x[F_DOCID]))
		[d.append (s) for s,d in zip (states, foundDocs)]
		return foundDocs

	def index (self, doc=None):
		'''
		Add to queue if possible or to index all documents whose docids in the queue.

		::param docs:: a dictionary, whose key is a docid, and value is its state 
		'''
		result = None
		if doc is None:
			last_docs = self._get_queue ()
			last_collection = self._fetch_doc_content (last_docs)
			self._reset_queue ()
			result = self._indexing.index (last_collection)
		else:
			if self._queue_add (doc):
				result = True
			else:
				last_docs = self._get_queue ()
				last_collection = self._fetch_doc_content (last_docs)
				result = self._indexing.index (last_collection)
				self._reset_queue ()
				if not self._queue_add (doc):
					raise ValueError ('Cannot add a document to queue.')
		if result is True:
			self._drop_queue ()		
		return result

	def retrieve (self, query):
		result = self.retrieval.retrieve (query)
		return result
