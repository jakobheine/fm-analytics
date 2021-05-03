FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 85

COPY ./scripts /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "85"]