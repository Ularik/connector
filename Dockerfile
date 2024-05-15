FROM python:3.11.5-slim-bullseye
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
EXPOSE 8000
RUN apt update -y
RUN apt-get install -y --no-install-recommends mc
RUN apt-get install -y --no-install-recommends build-essential

RUN mkdir -p /usr/www/logs
RUN mkdir -p /usr/www/media
#RUN mkdir -p /usr/www/.venv

RUN python -m pip install --upgrade pip
#RUN pip install pipenv

WORKDIR /usr/www/

COPY requirements.txt .
#COPY Pipfile .
RUN pip install -r requirements.txt

#RUN pipenv install

WORKDIR /usr/www/app/
COPY app .

# CMD ["python", "-m", "daphne", "-b", "0.0.0.0", "-p", "8000", "project.asgi:application"]