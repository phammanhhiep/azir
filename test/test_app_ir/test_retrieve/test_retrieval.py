import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.retrieval import Retrieval
from app_ir.retrieve.ranking import Ranking
from app_ir.indexing.indexing import Indexing
from app_ir.preprocess.preprocess import Preprocessing


import pytest, pymongo
from collections import defaultdict
from bson.objectid import ObjectId

@pytest.mark.skip
def test__fetch_postings_lists ():
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

	try:
		indexing = Indexing ('test')
		preprocessing = Preprocessing ()
		ranking = Ranking ()		
		indexing.vocabulary_coll.insert_many (vocabulary)
		indexing.index_coll.insert_many (index)
		indexing.create_cache ()
		r = Retrieval ('test', indexing=indexing, preprocessing=preprocessing, ranking=ranking)
		pl = r._fetch_postings_lists (tokens)

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
		indexing.vocabulary_coll.drop ()
		indexing.index_coll.drop ()
		assert False
	else:
		indexing.vocabulary_coll.drop ()
		indexing.index_coll.drop ()		

@pytest.mark.skip
def test__merge_postings_lists (): pass

@pytest.mark.skip
def test__fetch_docs (): pass

@pytest.mark.skip
def test_retrieve (): pass

@pytest.mark.skip
def test__merge_pl0 ():
	# Test: only one term found
	pls = [
		{'termid': 1, 'pl':[[0,1,11]]},
		{'termid': 3, 'pl': [[0,1,14]]},
		{'termid': 0, 'pl': [[0,1,10], [1,1,20]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
	]

	indexing = Indexing ('test')
	preprocessing = Preprocessing ()	
	ranking = Ranking ()		
	b = Retrieval ('test', indexing=indexing, preprocessing=preprocessing, ranking=ranking)
	merged_pls = b._merge_indexes ([pls[0]])

	expect_merged_pls = [[0,(1,1),[11]]]
	assert len (merged_pls) == len (expect_merged_pls)
	for i,j in zip (merged_pls, expect_merged_pls):
		assert len (i) == len (j)
		for t,k in zip (i,j):
			assert t == k

@pytest.mark.skip
def test__merge_pl1 ():
	# test more than one keyword found
	pls = [
		{'termid': 1, 'pl':[[0,1,11], [1,1,1]]},
		{'termid': 3, 'pl': [[0,1,14], [1,1,2]]},
		{'termid': 0, 'pl': [[0,2,10,15], [1,1,3]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
	]

	indexing = Indexing ('test')
	preprocessing = Preprocessing ()	
	ranking = Ranking ()		
	b = Retrieval ('test', indexing=indexing, preprocessing=preprocessing, ranking=ranking)
	merged_pls = b._merge_indexes (pls)
	merged_pls = sorted (merged_pls, key=lambda x: x[0]) # for testing only

	expect_merged_pls = [
		[0,[(0,2), (1,1), (2,1), (3,1)],[10, 11, 12, 14, 15]],
		[1,[(0,1), (1,1), (2,1), (3,1)],[1,2,3,12]],
	]

	assert len (merged_pls) == len (expect_merged_pls)
	for i,j in zip (merged_pls, expect_merged_pls):
		assert len (i) == len (j)
		for t,k in zip (i,j):
			assert t == k

