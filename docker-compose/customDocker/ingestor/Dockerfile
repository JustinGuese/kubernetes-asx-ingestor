FROM python:alpine
RUN mkdir /app
WORKDIR /app/
COPY ./src/requirements.txt /app
RUN pip install -r requirements.txt
COPY ./src/app.py /app/
COPY ./src/stocks.txt /app/
ENTRYPOINT [ "python" ,"app.py" ]