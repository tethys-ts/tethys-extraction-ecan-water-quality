FROM python:3.8-buster

# ENV TZ='Pacific/Auckland'

# RUN apt-get update && apt-get install -y unixodbc-dev gcc g++ libspatialindex-dev python-rtree

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY utils.py main.py site_data.py ts_data.py ./

CMD ["python", "main.py", "parameters.yml"]
