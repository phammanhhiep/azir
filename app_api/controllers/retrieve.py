from flask import jsonify, request

def retrieve (irsys):
	q = request.args.get ('q')
	results = irsys.retrieve (q)
	res = jsonify ({'data': {'results': results, 'q': q}})
	return res