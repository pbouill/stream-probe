FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache
RUN pip cache purge
COPY . .
ENTRYPOINT [ "python3 /app/main.py" ]