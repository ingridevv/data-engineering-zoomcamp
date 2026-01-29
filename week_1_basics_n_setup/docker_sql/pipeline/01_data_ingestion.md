## Initial draft - Learning Keynotes

### Step by step host access to postgres via container port mapping
- Enter the pipeline folder where it contains Dockerfile and pipeline.py
- Run the container to run postgres
- Run uv run pgcli -h localhost -p 5432 -u root -d ny_taxi

# Data processing with ipynb
- uv run jupyter notebook --no-browser --port=8888 //terminal bug
- open notebook.ipynb
- Convert notebook to py script: uv run jupyter nbconvert --to=script notebook.ipynb
mv notebook.py ingest_data.py