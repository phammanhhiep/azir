from app_api.db.contents import ContentModel
from app_api.db.indexes import IndexModel
from app_api.db.vocabularies import VocabularyModel
from app_api.db.content_vectors import ContentVectorModel
from app_api.db.queued_contents import QueuedConentModel

import pymongo

class MongoDB:
	def __init__ (self, dbname='ngheaz'):
		self.db = pymongo.MongoClient ()[dbname] 
		self.content_coll = ContentModel (self.db)
		self.index_coll = IndexModel (self.db)
		self.vocabulary_coll = VocabularyModel (self.db)
		self.contentvectors_coll = ContentVectorModel (self.db) # << later change to content_vector_coll >>
		self.queued_content_coll = QueuedConentModel (self.db)