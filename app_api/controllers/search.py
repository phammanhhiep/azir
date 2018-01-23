from flask import jsonify

def search (req):
	q = req.args.get ('q')
	res = jsonify ({'data': {'results': [], 'q': q}})
	return res