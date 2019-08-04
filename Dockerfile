FROM python:3.6
EXPOSE 8080
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["gunicorn", "-w", "9", "-b", "0.0.0.0:8080", "index:app"]