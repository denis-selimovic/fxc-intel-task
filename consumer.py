import datetime
import json
import threading
import time

import pika
import psycopg2
import redis
import redis.exceptions


def seconds_to_next_minute():
    return 60 - datetime.datetime.now().second


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


postgres = get_postgres_connection()
rabbitmq = get_rabbit_mq_connection()
redis_client = get_redis_connection()


def update_keydb():
    print(f"Starting update for keydb at {datetime.datetime.now()}")
    timer = threading.Timer(seconds_to_next_minute(), update_keydb)
    timer.start()

    data = redis_client.hgetall("aggregator")

    with redis_client.pipeline() as pipe:
        pipe.multi()

        for k, v in data.items():
            pipe.incr(k, int(v))
            pipe.hincrby("aggregator", k, -int(v))

        pipe.execute()


def process_event(ch, method, properties, body):
    print(f"Received event {body}")
    max_retries = 5
    data = json.loads(body)
    provider_name = None

    headers = properties.headers or {}
    retries = headers.get("x-retry", 0)
    psql_stored = headers.get("x-psql-stored", False)

    try:
        with postgres:
            with postgres.cursor() as cursor:
                cursor.execute(
                    "SELECT provider_name FROM initial_data where id = %s", (data["id"],)
                )
                result = cursor.fetchone()

                if result:
                    provider_name = result[0]

                    if psql_stored is False:
                        cursor.execute(
                            "INSERT INTO historical_transactions(provider_id, transaction_value) VALUES (%s, %s)",
                            (data["id"], data["value"])
                        )

        psql_stored = True

        if provider_name:
            redis_client.hincrby("aggregator", f"{data['id']}_{provider_name}", data["value"])

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Event {body} stored successfully. Sending ACK to rmq...")
    except Exception:
        if retries < max_retries:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"Failed to store event {body}. Requeuing the message.")
            
            headers = properties.headers or {}
            headers["x-retry"] = retries + 1
            headers["x-psql-stored"] = psql_stored

            ch.basic_publish(
                exchange="",
                routing_key="incoming_transactions",
                body=body,
                properties=pika.BasicProperties(headers=headers)
            )

        else:
            print(f"Max retries exceeded for {body}. Sending message to DLQ...")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def event_listener():
    channel = rabbitmq.channel()
    queue = "incoming_transactions"

    channel.queue_declare(queue=queue, durable=True)
    channel.basic_consume(queue=queue, on_message_callback=process_event)

    try:
        channel.start_consuming()
    except Exception:
        channel.stop_consuming()


def main():
    # set initial values based on seed data
    query = """
        select concat(i.id, '_', i.provider_name), sum(i.initial_value) + sum(coalesce(x.value, 0))
        from initial_data i 
        left join (
            select provider_id, sum(transaction_value) as value
            from historical_transactions
            group by provider_id
        ) x on x.provider_id = i.id
        group by concat(i.id, '_', i.provider_name);
    """

    with postgres:
        with postgres.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    
    with redis_client.pipeline() as pipe:
        pipe.multi()

        for row in result:
            pipe.set(row[0], int(row[1]))
        
        pipe.execute()

    try:
        thread = threading.Thread(target=event_listener)
        thread.start()

        event = threading.Timer(seconds_to_next_minute(), update_keydb)
        event.start()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
