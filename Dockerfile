FROM python:3.6.4
COPY . /app
WORKDIR /app
RUN pip install --upgrade pip; \
    pip install -r requirements.txt
CMD python ./webjob.py