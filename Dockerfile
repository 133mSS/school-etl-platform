FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install flask pandas

CMD ["python", "scripts/mock_api_server.py", "--port", "5055"]