FROM python

WORKDIR /app
COPY pipeline.py pipeline.py
RUN pip install pandas numpy regex sqlalchemy psycopg2 

ENTRYPOINT [ "python", "pipeline.py"]