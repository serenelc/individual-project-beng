FROM python:3-alpine
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
ADD tfl_predictions.py .
ADD cron.py .
RUN pip3 install psycopg2
RUN pip3 install python-crontab
CMD [ "python", "./cron.py" ]