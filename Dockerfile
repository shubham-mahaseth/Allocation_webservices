FROM python:3.8
ENV PYTHONUNBUFFERED 1

WORKDIR /webservices
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install django-extensions
RUN pip install numpy
RUN pip install pandas
RUN pip install pymysql
RUN pip install sqlalchemy
RUN pip3 install google-cloud-speech --upgrade
RUN pip install "cloud-sql-python-connector[pymysql]"
RUN python3 -m pip install PyMySQL
RUN pip install --upgrade google-cloud-speech
RUN pip install google-cloud
RUN pip install storage
RUN pip install bcrypt
COPY . .
CMD python manage.py runserver 0.0.0.0:8000


