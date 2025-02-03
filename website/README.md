# Iceberg.FYI Results Website


## Running the site locally

``` sh
uv run fastapi dev
```

Our deployment server (Render) does not support `uv`. If you add more dependencies, please update the `requirements.txt` file by running `uv pip freeze > requirements.txt`.