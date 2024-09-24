import datetime
import json

import pika


def update_keydb(redis_client):
    print(f"Starting update for keydb at {datetime.datetime.now()}")

    try:
        data = redis_client.hgetall("aggregator")

        with redis_client.pipeline() as pipe:
            pipe.multi()

            for k, v in data.items():
                pipe.incr(k, int(v))
                pipe.hincrby("aggregator", k, -int(v))

            pipe.execute()

    except Exception:
        print("Failed to update keydb")


def process_event(ch, method, properties, body, postgres, redis_client):
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
                    "SELECT provider_name FROM initial_data where id = %s",
                    (data["id"],),
                )
                result = cursor.fetchone()

                if result:
                    provider_name = result[0]

                    if psql_stored is False:
                        cursor.execute(
                            "INSERT INTO historical_transactions(provider_id, transaction_value) VALUES (%s, %s)",
                            (data["id"], data["value"]),
                        )

        psql_stored = True

        if provider_name:
            redis_client.hincrby(
                "aggregator", f"{data['id']}_{provider_name}", data["value"]
            )

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
                properties=pika.BasicProperties(headers=headers),
            )

        else:
            print(f"Max retries exceeded for {body}. Sending message to DLQ...")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
