import json
import math
from collections import defaultdict
from flask import Flask, abort, request, jsonify
from flask_basicauth import BasicAuth
from flask_swagger_ui import get_swaggerui_blueprint
import pymysql
from dotenv import load_dotenv
from pathlib import Path
import os

appIkram = Flask(__name__)
appIkram.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(appIkram)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi_sakila.yaml', 
)
appIkram.register_blueprint(swaggerui_blueprint)

MAX_PAGE_SIZE = 100

env_path = Path('/Users/ikram/Desktop') / '.env'
load_dotenv(dotenv_path=env_path)

def get_db_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password=os.getenv('DB_PASSWORD'),  # Utilise la variable d'environnement
        database="sakila",
        cursorclass=pymysql.cursors.DictCursor
    )

def remove_null_fields(obj):
    return {k: v for k, v in obj.items() if v is not None}

@appIkram.route("/actors/count-by-category")
@auth.required
def get_actors_count_by_category():
    db_conn = get_db_connection()
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT c.name AS category, COUNT(fa.actor_id) AS actor_count
            FROM category c
            JOIN film_category fc ON c.category_id = fc.category_id
            JOIN film f ON fc.film_id = f.film_id
            JOIN film_actor fa ON f.film_id = fa.film_id
            GROUP BY c.name
        """)
        results = cursor.fetchall()
    db_conn.close()
    return jsonify(results)

@appIkram.route("/categories")
@auth.required
def get_categories():
    db_conn = get_db_connection()
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT c.name AS category, COUNT(fc.film_id) AS film_count
            FROM film_category fc
            JOIN category c ON fc.category_id = c.category_id
            GROUP BY c.name
            ORDER BY film_count DESC
        """)
        categories = cursor.fetchall()
    db_conn.close()
    return jsonify(categories)

if __name__ == '__main__':
    appIkram.run(debug=True, host='0.0.0.0', port=5002)



# flask --app App3 run --port 5002


#Ensuite, dans le terminal du répertoire, penser à: 
# 1) se rendre dans le répertoire via le terminal (cd /Users/ikram/flaskapi/flask/flask)
# 2) activer l'environnement virtuel (source venv/bin/activate), 
# 3) exécuter le script: python App3.py
# 4) vérifier le résultat via le navigateur:http://localhost:5002/actors/count-by-category et http://localhost:5002/categories