from flask import request, send_from_directory, render_template
from app import app
import sqlite3

@app.route('/', methods=['GET'])
def home():
	return render_template('404.html'), 404

# robots.txt file
@app.route('/robots.txt', methods=['GET'])
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404