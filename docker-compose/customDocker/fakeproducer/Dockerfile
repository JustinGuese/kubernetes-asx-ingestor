FROM python:alpine
RUN mkdir /app
WORKDIR /app/
COPY ./src/requirements.txt /app
COPY ./src/logs_tradesprod_2021-01-25.txt.tar /app
RUN pip install -r requirements.txt
COPY ./src/app.py /app/
ENTRYPOINT [ "python" ,"app.py" ]