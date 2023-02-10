FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# Specifying workdir for docker container
# Copy file pipeline.py from local to workdir inside container with following filename
WORKDIR /app 
COPY ingest_data.py ingest_data.py

# ENTRYPOINT [ "bash" ] 
ENTRYPOINT ["python", "ingest_data.py"]