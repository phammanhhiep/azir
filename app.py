from flask import Flask
from flask_script import Manager 
from flask_cors import CORS
import app_api.routes.index as api_routes
from app_ir.irsys import IRSYS

app = Flask (__name__, template_folder='app_server/templates/')
app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS (app, resources={r"/api/*": {"origins": "http://localhost:30000"}})
irsys = IRSYS ()

api_routes.route (app, irsys);

if __name__ == '__main__':
	app.run (debug=True)
	# Manager (app).run ()