import multiprocessing
import time

from apscheduler.schedulers.background import BackgroundScheduler

from connection import (
    get_postgres_connection,
    get_rabbit_mq_connection,
    get_redis_connection,
)
from util import process_event, update_keydb


def aggregator():
    redis_client = get_redis_connection()
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_keydb, args=(redis_client,), trigger="cron", second=0)
    scheduler.start()

    try:
        while True:
            time.sleep(0.5)
    except Exception:
        scheduler.shutdown()


def event_listener(postgres, redis_client, rabbitmq):
    channel = rabbitmq.channel()
    queue = "incoming_transactions"

    def callback(ch, method, properties, body):
        return process_event(ch, method, properties, body, postgres, redis_client)

    channel.queue_declare(queue=queue, durable=True)
    channel.basic_consume(queue=queue, on_message_callback=callback)

    try:
        channel.start_consuming()
    except Exception:
        channel.stop_consuming()


postgres = get_postgres_connection()
redis_client = get_redis_connection()
rabbitmq = get_rabbit_mq_connection()


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

    max_attempts = 10

    for _ in range(max_attempts):
        try:
            with postgres:
                with postgres.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()

            break
        except Exception:
            print("Failed to load seed data. Trying again...")

    for _ in range(max_attempts):
        try:
            with redis_client.pipeline() as pipe:
                pipe.multi()

                for row in result:
                    pipe.set(row[0], int(row[1]))

                pipe.execute()

            break
        except Exception:
            print("Failed to set initial data in keydb. Trying again...")

    try:
        aggregator_proc = multiprocessing.Process(target=aggregator)
        aggregator_proc.start()

        event_listener(postgres, redis_client, rabbitmq)
    except (KeyboardInterrupt, SystemExit):
        aggregator_proc.terminate()
        aggregator_proc.join()


if __name__ == "__main__":
    main()
