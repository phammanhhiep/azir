from flask import jsonify, request

def index (irsys):
	data = request.get_json ()
	results = irsys.index (data['data'])
	res = jsonify ({'data': results})
	return res