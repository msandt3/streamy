## Streamy
This is a simple toy project I build to play with streaming patterns. Its designed to mimic a production multi-app architecture. 

The components are as follows:
- Cluster - this is the Kafka Cluster
- Producer - this is a simple app that reads the wiki event stream and pipes those to a Kafka Topic
- Consumer - this is the "client" application. Here we read records off the stream and pipe batches to be loaded into duckdb. 
- Right now its not doing anything other than writing to duckdb. 

Getting it up & Running

You can start the cluster & producer logic with docker compose
```bash
docker compose build && docker compose up
```

Then you can run the client application - not dockerized (yet)

```bash
cd consumer/
pip install -r requirements.txt
python kafka_pipeline.py
```

Once you shut down the consumer - you can inspect the loads with

```bash
dlt pipeline kafka_pipeline show
```


Run the tests

```bash
python -m pytest consumer/tests/*.py
```