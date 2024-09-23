"""
Oi, hello, fellow developer!
Of course you can use everything provided in this script to your advantage!
"""
import psycopg2
import pika
import json
import time
import random


def main():
    ORG_COUNT = 2
    print("Running DB init")

    # Establish a connection to Postgres
    attempt_count = 0
    while attempt_count < 10:
        try:
            # Connection details
            conn = psycopg2.connect(
                dbname="fxc",
                user="user",
                password="password",
                host="postgres",
                port="5432"
            )
            break
        except psycopg2.OperationalError:
            attempt_count +=1
            print(f"reconnecting to postgres, {attempt_count=}")
            time.sleep(2)
    else:
        print("Couldn't connect to postgres")
        return

    # Creating a cursor object to interact with the database
    cur = conn.cursor()

    # Create the initial_data table only if it does not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS initial_data (
            id SERIAL PRIMARY KEY,
            provider_name VARCHAR(255) NOT NULL,
            initial_value NUMERIC NOT NULL
        );
    """)

    # Create the historical_transactions table only if it does not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS historical_transactions (
            id SERIAL PRIMARY KEY,
            provider_id INTEGER REFERENCES initial_data(id),
            transaction_value NUMERIC NOT NULL
        );
    """)

    # Insert data into initial_data table
    cur.execute("""
        INSERT INTO initial_data (provider_name, initial_value) VALUES
        ('Visa', 1000),
        ('Mastercard', 2000)
        ON CONFLICT DO NOTHING;
    """)

    # Insert data into historical_transactions table
    cur.execute("""
        INSERT INTO historical_transactions (provider_id, transaction_value) VALUES
        (1, 100),
        (1, 200),
        (2, -200)
        ON CONFLICT DO NOTHING;
    """)

    # Commit the transaction
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

    print("DB init complete, starting to produce txs")
    # Connection parameters (replace with your RabbitMQ settings if necessary)
    rabbitmq_host = 'rabbitmq'
    rabbitmq_port = 5672
    rabbitmq_user = 'user'
    rabbitmq_password = 'password'
    queue_name = 'incoming_transactions'

    # Establish a connection to RabbitMQ
    attempt_count = 0
    while attempt_count < 10:
        try:
            credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
            channel = connection.channel()
            print("connected to rmq")
            break
        except pika.exceptions.AMQPConnectionError:
            attempt_count +=1
            print(f"reconnecting to rmq, {attempt_count=}")
            time.sleep(2)
    else:
        print("Couldn't connect to rmq")
        return

    # Declare the queue
    channel.queue_declare(queue=queue_name, durable=True)

    # Function to publish a message
    def publish_message(id, value):
        message = {
            "id": id,
            "value": value
        }
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        print(f" [x] Sent {message}")

    while True:
        publish_message(random.randint(1, ORG_COUNT), random.randint(-1000, 1000))
        time.sleep(random.randint(10, 40))

if __name__ == "__main__":
    main()
