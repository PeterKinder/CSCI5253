FROM python

WORKDIR /app
COPY pipeline.py pipeline_c.py
RUN pip install pandas
RUN pip install numpy

ENTRYPOINT [ "bash" ]