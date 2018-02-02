import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.irsys import IRSYS
from app_api.db.db import MongoDB
import pytest, pymongo
from bson.objectid import ObjectId

db = MongoDB (dbname='test')

@pytest.mark.skip
def test_index ():
	# Test: when doc is None and no index exists
	irsys = IRSYS (db=db)
	result = irsys.index ()
	assert result is None

@pytest.mark.skip
def test_index2 ():
	# Test: when doc is None and no index exists

	contents = ['xx yy zz. xx tt.', 'yy yy zz. zz tt kk.']
	db.content_coll.insert_many (contents)
	docs = db.content_coll.find_many ()
	docs = list (docs)
	docs = [[d['_id'], 0] for d in docs]
	irsys = IRSYS (db=db)
	try:
		result = irsys.index (docs)
		assert result
		db.content_coll.drop ()
		db.index_coll.drop ()
		db.contentvectors_coll.drop ()
		db.vocabulary_coll.drop ()
	except Exception as ex:
		db.content_coll.drop ()
		db.index_coll.drop ()
		db.contentvectors_coll.drop ()
		db.vocabulary_coll.drop ()
		assert False

@pytest.mark.skip
def test_index3 ():
	# Test: when doc is not None and doc queue equals max number of docs in queue.

	contents = ['xx yy zz. xx tt.', 'yy yy zz. zz tt kk.', 'yy yy zz. zz tt kk.']
	db.content_coll.insert_many (contents)
	docs = db.content_coll.find_many ()
	docs = list (docs)
	prev_docs = docs[:-1]
	doc = docs[-1]
	doc = [doc['_id'], doc]
	prev_docs = [[d['_id'], 0] for d in prev_docs]
	irsys = IRSYS (db=db, max_queue=2)
	try:
		[irsys._queue_add (d) for d in prev_docs]
		result = irsys.index (doc)
		assert result
		db.content_coll.drop ()
		db.index_coll.drop ()
		db.contentvectors_coll.drop ()
		db.vocabulary_coll.drop ()
	except Exception as ex:
		db.content_coll.drop ()
		db.index_coll.drop ()
		db.contentvectors_coll.drop ()
		db.vocabulary_coll.drop ()
		assert False	

@pytest.mark.skip
def test_save_queue (): pass

@pytest.mark.skip
def test_retrieve ():
	'''
	No need to test since the method is just a wrapper of the retrieve method of retrieval object.
	The method has been test.
	'''
	assert True