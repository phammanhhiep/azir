import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.ranking import Ranking, WeightedZoneScoring, TermFrequencyScoring, TFIDFScoring, CosineScoring
import math
from collections import defaultdict
import pytest

@pytest.mark.zoneScoreTesting
@pytest.mark.skip
def test_zs_score (): 
	pl = [
		[0, (0,1)], 
		[2, (0,1,2)],
		[4, (2,)]
	]

	zone_weights = {
		0: 0.4,
		1: 0.1,
		2: 0.5
	}

	expected_scores = [[2, 1], [0, 0.5], [4, 0.5]]

	s = WeightedZoneScoring (zone_weights)
	scores = s.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d

@pytest.mark.zoneScoreTesting
@pytest.mark.skip
def test_zs_train ():
	assert False

@pytest.mark.termFrequencyTesting
@pytest.mark.skip
def test_tf_score ():
	pl = [
		[0, (1,5,2)], 
		[2, (2,3,11)],
		[4, (1,1)]
	]

	expected_scores = [[2, 16], [0, 8], [4, 2]]

	t = TermFrequencyScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d	

@pytest.mark.tfidfTesting
@pytest.mark.skip
def test_tfidf_score ():
	def idf (df,N):
		return math.log (N/df, 10)

	pl = [
		[0, (100,(20,1),(10,5),(5,2))], 
		[2, (100,(32,2),(12,3),(13,11))],
		[4, (100,(11,1),(10,5),(50,2))],
	]

	expected_scores = [
		[2, (2 * idf(32,100)) + (3 * idf(12,100)) + (11 * idf(13,100))], 
		[0, (1 * idf(20,100)) + (5 * idf(10,100)) + (2 * idf(5,100))], 
		[4, (1 * idf(11,100)) + (5 * idf(10,100)) + (2 * idf(50,100))]
	]

	t = TFIDFScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d

@pytest.mark.cosineScoringTesting
@pytest.mark.skip
def test_cosine_score ():
	def idf (df,N):
		return math.log (N/df, 10)

	pl = [
		[None, (100, None, (20,1), (11,1), (20,1), (4,2))],
		[0, (100, 20, (20,0), (11,15), (20,0), (4,1))], 
		[2, (100, 10, (20,1), (11,0), (20,0), (4,20))],
		[4, (100, 100, (20,0), (11,0), (20,5), (4,7))],
	]

	expected_scores = [
		[2, ((1 * 1 * idf(20,100)* idf(20,100)) + 0 + 0 + (2 * 20 * idf(4,100)* idf(4,100))) / 10],
		[0, (0 + (15 * 1 * idf(11,100) * idf(11,100)) + 0 + (1 * 2 * idf(4,100) * idf(4,100))) / 20],
		[4, (0 + 0 + ( 1 * 5 * idf(20,100)* idf(20,100)) + (2 * 7 * idf(4,100)* idf(4,100))) / 100],
	]

	t = CosineScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d

@pytest.mark.cosineScoringTesting
@pytest.mark.skip
def test_cosine_docv_len ():
	vocabulary = {
		'xx': {'df': 10, 'termid': 0},
		'yy': {'df': 15, 'termid': 4},
		'zz': {'df': 30, 'termid': 100},
		'tt': {'df': 45, 'termid': 101},
		'kk': {'df': 10, 'termid': 1000}
	}
	D = 10
	docv = [('xx', 1), ('yy', 3), ('zz', 5), ('tt', 2)]
	t = CosineScoring ()
	dl = t._docv_len (docv, vocabulary, D)

	expected = math.sqrt (t._tfidf (D, 10, 1)**2 + t._tfidf (D, 15, 3)**2 
		+ t._tfidf (D, 30, 5)**2 + t._tfidf (D, 45, 2)**2)

	assert dl == expected

@pytest.mark.cosineScoringTesting
@pytest.mark.skip
def test_cosine_create_scoring_data ():
	'''
	Assume docv_len is calculated correctly, and thus not being tested.
	'''

	vocabulary = {
		'xx': {'df': 10, 'termid': 0},
		'yy': {'df': 15, 'termid': 4},
		'zz': {'df': 30, 'termid': 100},
		'tt': {'df': 45, 'termid': 101},
		'kk': {'df': 10, 'termid': 1000}
	}

	vocabulary = defaultdict (lambda: {'df': 0, 'termid': None}, vocabulary)

	class Indexing:
		def __init__ (self, vocabulary):
			self._vocabulary = vocabulary
		
		def get_vocabulary (self):
			return self._vocabulary

		def get_doc_vector (self, docid):
			return	[
				{'docid': 0, 'tf': [('xx', 1), ('yy', 3), ('zz', 5), ('tt', 2)]},
				{'docid': 1, 'tf': [('xx', 10), ('yy', 1), ('zz', 2), ('tt', 4)]},
				{'docid': 2, 'tf': [('xx', 1), ('yy', 1), ('zz', 1), ('tt', 3)]},
			]

	class Retrieval:
		def __init__ (self, indexing=None):
			self.D = 10
			self.indexing = indexing

	pl = [
		[0, [(0,1), (4,5), (100,2), (101,4)], []],
		[1, [(0,2), (4,2), (101,1)], []],
		[2, [(4,1), (100,1), (101,2)], []],
	]

	# Test: all terms query in vocabulary. 

	tokens = ['yy', 'xx', 'zz', 'tt']
	i = Indexing (vocabulary)
	r = Retrieval (indexing=i)
	t = CosineScoring ()
	scoring_data = t.create_scoring_data (r, pl, tokens)

	expected_score_data = [ # the scoring is incorrect order. But doing so makes it easier to tests.
		[None, 10, None, (10, 1), (15, 1), (30, 1), (45,1)],
		[0, 10, 10, (10,1),( 15,5), (30, 2), (45,4)],
		[1, 10, 20, (1, 0), (10,2), (15,2), (45,1)],
		[2, 10, 30, (1,0), (15,1), (30, 1), (45,2)],
	]

	# sort for testing
	for d in scoring_data:
		d[3:] = sorted (d[3:], key=lambda x: x[0])

	assert len (scoring_data) == len (expected_score_data)
	for a,b in zip (scoring_data, expected_score_data):
		assert len (a) == len (b)
		for c,d in zip (a[:2],b[:2]):
			assert c == d

		for c,d in zip (a[3:], b[3:]):
			assert len (c) == len (d)
			for e,f in zip (c,d):
				assert e == f