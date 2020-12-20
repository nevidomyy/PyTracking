FROM python:3.8

RUN mkdir -p /usr/src/app/
WORKDIR /usr/src/app/

COPY . /usr/src/app/
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

ENV TZ Europe/Moscow

CMD ["python", "main.py"]