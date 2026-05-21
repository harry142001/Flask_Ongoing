import logging
import os

from flask import Flask
from flask_cors import CORS

from cache import load_cache
from database import create_indexes
from routes.analytics import analytics_bp
from routes.comparables import comparables_bp
from routes.details import details_bp
from routes.export import export_bp
from routes.meta import meta_bp
from routes.search import search_bp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
CORS(app)

app.register_blueprint(meta_bp)
app.register_blueprint(comparables_bp)
app.register_blueprint(search_bp)
app.register_blueprint(details_bp)
app.register_blueprint(export_bp)
app.register_blueprint(analytics_bp)

create_indexes()
load_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)