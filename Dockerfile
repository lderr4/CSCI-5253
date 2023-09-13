FROM python:3.8


WORKDIR /app

COPY pipeline.py pipeline.py

RUN pip install pandas


ENTRYPOINT [ "python", "pipeline.py"]