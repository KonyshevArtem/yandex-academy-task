FROM python:3.6
EXPOSE 8080
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "index.py"]