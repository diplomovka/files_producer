FROM python:3.8.16-slim-bullseye

WORKDIR /app

COPY requirements.txt /app

RUN pip install -r requirements.txt

COPY . ./

EXPOSE 5000

CMD ["python", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5000"]