FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["functions-framework", "--target=process_nfe_xml", "--port=8080"]