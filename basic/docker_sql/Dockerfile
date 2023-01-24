FROM python:3.9

RUN pip install pandas

# Specifying workdir for docker container
# Copy file pipeline.py from local to workdir inside container with following filename
WORKDIR /app 
COPY pipeline.py pipeline.py

# ENTRYPOINT [ "bash" ]
ENTRYPOINT ["python", "pipeline.py"]