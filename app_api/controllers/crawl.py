from flask import jsonify, request

def save_job_posting (db):
	'''
	'''
	data = request.get_json ()
	# db.raw_contents.insert_many (data)
	res = jsonify ({'msg': 'OK'})
	return res

def extract_job_posting (data): pass

def _extract_author (): pass

def _extract_posting (): pass

def _extract_comment (): pass

def _save_job_posting (data): pass