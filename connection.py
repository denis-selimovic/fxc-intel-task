import time

import pika
import psycopg2
import redis
import redis.exceptions


def get_postgres_connection():
    attempt_count = 0
    conn = None

    while attempt_count < 10:
        try:
            conn = psycopg2.connect(
                dbname="fxc",
                user="user",
                password="password",
                host="postgres",
                port="5432",
            )
            break
        except psycopg2.OperationalError:
            attempt_count += 1
            print(f"reconnecting to postgres, {attempt_count=}")
            time.sleep(2)

    return conn


def get_rabbit_mq_connection():
    rabbitmq_host = "rabbitmq"
    rabbitmq_port = 5672
    rabbitmq_user = "user"
    rabbitmq_password = "password"

    attempt_count = 0
    connection = None

    while attempt_count < 10:
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host, port=rabbitmq_port, credentials=credentials
                )
            )
            break
        except pika.exceptions.AMQPConnectionError:
            attempt_count += 1
            print(f"reconnecting to rmq, {attempt_count=}")
            time.sleep(2)

    return connection


def get_redis_connection():
    host = "keydb"
    port = 6379
    attempt_count = 0
    connection = None

    while attempt_count < 10:
        try:
            connection = redis.Redis(host=host, port=port)
            break
        except redis.exceptions.ConnectionError:
            attempt_count += 1
            print(f"reconnecting to keydb, {attempt_count=}")
            time.sleep(2)

    return connection
