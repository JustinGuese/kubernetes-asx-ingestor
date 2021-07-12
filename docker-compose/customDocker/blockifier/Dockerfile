FROM python:3.9
RUN mkdir /app
WORKDIR /app/
COPY ./src/requirements.txt /app
RUN pip install -r requirements.txt
COPY ./src/app.py /app/
COPY ./src/asxmarketcaps.json /app/
ENTRYPOINT [ "python" ,"app.py" ]