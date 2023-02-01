FROM python:slim

# Install Python dependencies.
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /app
COPY ./app /app
WORKDIR /app

CMD ["python", "app.py"]