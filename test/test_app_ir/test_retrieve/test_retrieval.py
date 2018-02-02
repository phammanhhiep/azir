import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.retrieval import Retrieval
from app_ir.retrieve.ranking import CosineScoring
from app_ir.indexing.indexing import Indexing
from app_ir.preprocess.preprocess import Preprocessing
from app_api.db.db import MongoDB

from collections import defaultdict
from bson.objectid import ObjectId

import pytest

db = MongoDB ('test')

@pytest.mark.skip
def test__fetch_indexes ():
	'''
	Test: Fetch existing terms
	'''

	vocabulary = [
		{'termid': 0, 'term': 'xx', 'df': 1},
		{'termid': 1, 'term': 'yy', 'df': 1},
		{'termid': 2, 'term': 'zz', 'df': 1},
		{'termid': 3, 'term': 'tt', 'df': 1},
	]

	index = [
		{'termid': 0, 'pl': [[0,1,10], [1,1,20]]},
		{'termid': 1, 'pl': [[0,1,11]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
		{'termid': 3, 'pl': [[0,1,14]]},
	]

	tokens = ['xx', 'yy', 'zz', 'tt']

	expected_pl = [
		{'termid': 1, 'pl':[[0,1,11]]},
		{'termid': 3, 'pl': [[0,1,14]]},
		{'termid': 0, 'pl': [[0,1,10], [1,1,20]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
	]

	indexing = Indexing (db=db)
	preprocessing = Preprocessing ()
	ranking = CosineScoring ()		
	indexing._vocabulary_coll.insert_many (vocabulary)
	indexing._index_coll.insert_many (index)
	indexing.create_cache ()
	r = Retrieval (db=db, indexing=indexing, preprocessing=preprocessing, ranking=ranking)	

	try:
		pl = r._fetch_indexes (tokens)

		assert len (pl) == len (expected_pl)
		for a,b in zip (pl, expected_pl):
			assert a['termid'] == b['termid']
			assert len (a['pl']) == len (b['pl'])
			for c,d in zip (a['pl'],b['pl']):
				assert len (c) == len (d)
				for e,f in zip (c,d):
					assert e == f

	except Exception as ex:
		print (ex)
		indexing._vocabulary_coll.drop ()
		indexing._index_coll.drop ()
		assert False
	else:
		indexing._vocabulary_coll.drop ()
		indexing._index_coll.drop ()		

@pytest.mark.skip
def test__merge_postings_lists (): pass

@pytest.mark.skip
def test__fetch_docs (): pass

@pytest.mark.skip
def test__merge_pl0 ():
	# Test: only one term found
	indexes = [
		{'termid': 1, 'pl':[[0,1,11]]},
		{'termid': 3, 'pl': [[0,1,14]]},
		{'termid': 0, 'pl': [[0,1,10], [1,1,20]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
	]

	indexing = Indexing (db=db)
	preprocessing = Preprocessing ()	
	ranking = CosineScoring ()		
	b = Retrieval (db=db, indexing=indexing, preprocessing=preprocessing, ranking=ranking)
	merged_indexes, docids = b._merge_indexes ([indexes[0]])

	expected_merged_indexes = [[0,(1,1),[11]]]
	assert len (merged_indexes) == len (expected_merged_indexes)
	for i,j in zip (merged_indexes, expected_merged_indexes):
		assert len (i) == len (j)
		for t,k in zip (i,j):
			assert t == k

@pytest.mark.skip
def test__merge_pl1 ():
	# test more than one keyword found
	indexes = [
		{'termid': 1, 'pl':[[0,1,11], [1,1,1]]},
		{'termid': 3, 'pl': [[0,1,14], [1,1,2]]},
		{'termid': 0, 'pl': [[0,2,10,15], [1,1,3]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
	]

	indexing = Indexing (db=db)
	preprocessing = Preprocessing ()	
	ranking = CosineScoring ()		
	b = Retrieval (db=db, indexing=indexing, preprocessing=preprocessing, ranking=ranking)
	merged_indexes, docids = b._merge_indexes (indexes)
	merged_indexes = sorted (merged_indexes, key=lambda x: x[0]) # for testing only

	expected_merged_indexes = [
		[0,[(0,2), (1,1), (2,1), (3,1)],[10, 11, 12, 14, 15]],
		[1,[(0,1), (1,1), (2,1), (3,1)],[1,2,3,12]],
	]

	assert len (merged_indexes) == len (expected_merged_indexes)
	for i,j in zip (merged_indexes, expected_merged_indexes):
		assert len (i) == len (j)
		for t,k in zip (i,j):
			assert t == k

@pytest.mark.skip
def test_rank ():
	docs = [
		[1, 'xx xx'], [3, 'yy xy'], [7, 'xx zz']
	]

	scores = [
		[3, 5], [1,4], [7,1]
	]

	indexing = Indexing (db=db)
	preprocessing = Preprocessing ()	
	ranking = CosineScoring ()	
	b = Retrieval (db=db, indexing=indexing, preprocessing=preprocessing, ranking=ranking)	

	ranked_docs = b._rank (docs, scores)

	exp_ranked_docs = [
		[3, 'yy xy'], [1, 'xx xx'], [7, 'xx zz']
	]

	assert len (ranked_docs) == len (exp_ranked_docs)
	for a,b in zip (ranked_docs, exp_ranked_docs):
		assert len (a) == len (b) 
		for c,d in zip (a, b):
			assert c == d

@pytest.mark.skip
def test_retrieve ():
	q = 'tt yy zz'

	collection = ['xx yy zz. xx tt.', 'yy yy zz. zz tt kk.', 'kk gh mk']
	db.content_coll.insert_many (collection)
	collection = list (db.content_coll.find_many ())
	collection = [[d['_id'], d['content'], 0] for d in collection]

	preprocessing = Preprocessing ()	
	ranking = CosineScoring ()	
	indexing = Indexing (db=db, preprocessing=preprocessing)
	indexing.index (collection)	
	b = Retrieval (db=db, indexing=indexing, preprocessing=preprocessing, ranking=ranking)
	
	try:
		ranked_docs = b.retrieve (q)
		ranked_docsids = [d[0] for d in ranked_docs]
		expected_docids = [collection[1][0], collection[0][0]]

		assert len (ranked_docsids) == len (expected_docids)
		for a,b in zip (ranked_docsids, expected_docids):
			assert a == b

		db.index_coll.drop () 
		db.vocabulary_coll.drop ()	
		db.contentvectors_coll.drop ()
		db.content_coll.drop ()		
	except Exception as ex:
		print (ex)
		db.index_coll.drop () 
		db.vocabulary_coll.drop ()	
		db.contentvectors_coll.drop ()
		db.content_coll.drop ()
		assert False