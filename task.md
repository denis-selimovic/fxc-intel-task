# Python app building

This repository contains a `docker-compose` which starts up postgres, keydb (Redis), 
rabbitmq and an example producer app which populates these services with some data.

Your task is to create a separate repo and write an application in Python,
which interacts with these services.

## Running this application stack

Install `docker-compose` and run with `docker-compose up --build`

## External services description

Credentials for postgres and rabbitmq: `user:password`. Keydb is configured with
protected mode disabled, so no credentials needed there.

1. In postgres there are following tables:
```
historical_transactions

id pk, provider_id fk, transaction_value
1, 1, 100
2, 1, 200
3, 2, -200

initial_data

id pk, provider_name, initial_value
1, Visa, 1000
2, Mastercard, 2000
```
2. Keydb is initially empty, but is expected to have values in the following format:
```
key, value
1_Visa, 1300
2_Mastercard, 1800
```
3. In rabbitmq there is a queue `incoming_transactions`, where incoming txs are sent to. Message format is as follows:
```
{
"id": 1,
"value": 600
}
```

## App logic

Subscribe to rmq queue `incoming_transactions`, read message data, write it to postgres table `historical_transactions`. Each 60s (at 00:00:00, 00:01:00 etc) update current value for each provider in keydb.

After processing example message, keydb data should look like this:
```
key, value
1_Visa, 1900
2_Mastercard, 1800
```
And `historical_transactions` in postgres:
```
id pk, provider_id fk, transaction_value
1, 1, 100
2, 1, 200
3, 2, -200
4, 1, 600
```

## Constraints and conditions

- Only one instance of application is ruining at the same time
- No other applications are using specified external services
- Values in Postgres and RMQ are correct and don't contradict each other (e.g. id not present in postgres won't appear in RMQ messages)
- Tolerance for update time of KeyDB data is ±1 second against system clock.

