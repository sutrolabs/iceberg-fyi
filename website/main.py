from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
import yaml
import random
import urllib.parse

app = FastAPI(openapi_url=None)

templates = Jinja2Templates(directory="templates")

# Load data (in this case, from yaml files)
def load_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        return data

DATABASE_PATH = "../database"

results = load_yaml(f"{DATABASE_PATH}/results.yml")["results"]
posts = load_yaml(f"{DATABASE_PATH}/posts.yml")["posts"]
object_stores = load_yaml(f"{DATABASE_PATH}/storage.yml").keys()
query_engines = load_yaml(f"{DATABASE_PATH}/query_engine.yml").keys()
catalogs = load_yaml(f"{DATABASE_PATH}/catalog.yml").keys()


def calculate_metrics(object_store=None, query_engine=None, catalog=None):
    query_engine_counts = {}

    fully_qualified = query_engine is not None and catalog is not None and object_store is not None

    if query_engine is None or fully_qualified:
        for result in results:
            if object_store and result["storage"] != object_store: continue
            if catalog and result["catalog"] != catalog: continue

            if result["query_engine"] in query_engine_counts:
                query_engine_counts[result["query_engine"]] += 1
            else:
                query_engine_counts[result["query_engine"]] = 1

    object_store_counts = {}
    if object_store is None or fully_qualified:
        for result in results:
            if query_engine and result["query_engine"] != query_engine: continue
            if catalog and result["catalog"] != catalog: continue

            if result["storage"] in object_store_counts:
                object_store_counts[result["storage"]] += 1
            else:
                object_store_counts[result["storage"]] = 1

    catalog_counts = {}
    if catalog is None or fully_qualified:
        for result in results:
            if query_engine and result["query_engine"] != query_engine: continue
            if object_store and result["storage"] != object_store: continue

            if result["catalog"] in catalog_counts:
                catalog_counts[result["catalog"]] += 1
            else:
                catalog_counts[result["catalog"]] = 1

    return {
        "query_engine_metrics": query_engine_counts,
        "object_store_metrics": object_store_counts,
        "catalog_metrics": catalog_counts
    }


@app.get("/")
async def index(request: Request):
    object_store = request.query_params.get('object_store') or None
    query_engine = request.query_params.get('query_engine') or None
    catalog = request.query_params.get('catalog') or None

    test_results = None

    if object_store is not None and query_engine is not None and catalog is not None:
        test_results = list(filter(lambda result: result["storage"] == object_store and result["query_engine"] == query_engine and result["catalog"] == catalog, results))

    metrics = calculate_metrics(object_store, query_engine, catalog)

    filtered_posts = posts
    if object_store is not None or query_engine is not None or catalog is not None:
        filtered_posts = []
        for post in posts:
            if (object_store is not None and 'storage:' + object_store in post["related_to"]) or \
               (query_engine is not None and 'query_engine:' + query_engine in post["related_to"]) or \
               (catalog is not None and 'catalog:' + catalog in post["related_to"]):
                    filtered_posts.append(post)


    return templates.TemplateResponse("index.html", {
        "request": request,
        "query_engines": query_engines,
        "object_stores": object_stores,
        "catalogs": catalogs,
        "test_results": test_results,
        "metrics": metrics,
        "posts": filtered_posts[:5]
    })

@app.get("/random")
async def get_random(request: Request):

    random_object_store = random.choice(list(object_stores))
    random_query_engine = random.choice(list(query_engines))
    random_catalog = random.choice(list(catalogs))

    params = {
        "object_store": random_object_store,
        "query_engine": random_query_engine,
        "catalog": random_catalog
    }

    url = f"/?{urllib.parse.urlencode(params)}"

    return RedirectResponse(url, status_code=302)

app.mount("/static", StaticFiles(directory="static"), name="static")


