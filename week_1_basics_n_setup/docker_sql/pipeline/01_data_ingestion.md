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

## Running the script ingest_data.py from our host
uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips


## Container ingestion 
- Is taking the script from ingest_data.py
docker run -it --rm \
    taxi_ingest:v001 \
        --pg-user=root \
        --pg-pass=root \
        --pg-port=5432 \
        --pg-db=ny_taxi \
        --target-table=yellow_taxi_trips \
        --chunksize=100000

## Creating a docker network for error
'sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) connection to server at "localhost" (::1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
        Is the server running on that host and accepting TCP/IP connections?'

docker network create pg-network

# Run PostgreSQL on the network
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips

# Reverse Github proxy blocking pgadmin 
docker rm -f pgadmin

docker run -d \
  --name pgadmin \
  --network=pg-network \
  -p 8085:80 \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -e PGADMIN_CONFIG_SERVER_MODE="False" \
  -e PGADMIN_CONFIG_PROXY_X_HOST_COUNT=1 \
  -e PGADMIN_CONFIG_PROXY_X_PREFIX_COUNT=1 \
  --user root \
  dpage/pgadmin4

# Check all the networks we have
docker network ls 

# Run the scrip (pipeline-default)
docker run -it --rm\
  --network=pipeline_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips