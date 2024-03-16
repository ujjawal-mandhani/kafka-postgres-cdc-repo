FROM python:3.12
RUN pip install requests confluent_kafka
COPY kafka-postgres-connection.py /home/app/kafka-postgres-connection.py
CMD [ "python3", "-u", "/home/app/kafka-postgres-connection.py" ]
# CMD ["sh", "-c", "tail -f /dev/null"]  
# USE ABOVE COMMAND FOR DEBUG