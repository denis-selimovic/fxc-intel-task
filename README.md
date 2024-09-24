# FXC Intel Task

## Commands

You can start all external services and consumers by just running `docker-compose up --build`

## Code layout

Code is structured inside three different files

- connection.py contains three functions used to connect to RabbitMQ, keydb and PostgreSQL respecively
- util.py contains two main components of the program:
  - Event listener for RabbitMQ in `process_event` method
  - Peridic task `update_keydb` for updating keydb on every full minute
- consumer.py is entrypoint to the program. It starts consuming on RabbitMQ queue in the main process (single thread) and starts one more thread in another process for scheduled keydb sync

## connection.py

Pretty simple code. Three methods implement connections to RabbitMQ, keydb and PostgreSQL. There are 10 attempts made at the most to try and connect to the respective database/broker. I've added multiple retries, if needed, so we don't get errors when starting with `docker-compose`

## util.py

Event listener for RabbitMQ queue `incoming_transactions` is implemented in the `process_event` function. This function will write new event to the `historical_transactions` database. It will also update `aggregator` hash in keydb which holds counter for each provider between two syncs to keydb.

There are three possible code execution paths in this method:

- both writing to PostgreSQL and keydb are successful
- PostgreSQL insert succeeds, but write to keydb fails
- PostgreSQL insert fails

First case is straightforward and what we expect to happen 99% of the times. When this happens, we just send acknowledgement back to the RabbitMQ. If write to PostgreSQL succeeds, but write to redis somehow failed we will retry the event again by requeueing it back to the queue. When this event reappears again the queue we will read message headers. If headers say that the event was already stored in PostgreSQL we won't save it again, but we will only try to save it in keydb. A maximum of 5 retries is allowed, otherwise event will be sent to DLQ (another improvement would be to also remove transaction from table).

If even PostgreSQL fails, we will just retry again as in the previous case, but on retry we will again try to write to PostgreSQL. For this case we also allow maximum of 5 retries. We use headers from the message to store information about retries and suceede transaction.

I've opted to accumulate data between syncs in keydb `hash` data structure which will hold one counter per provider. This way, actual syncing to the requested keys in keydb will be pretty fast because we accumulate intermediate data. Another approach that I considered was using additional field in `historical_transactions` table to be able to determine which transactions were processed. (I wasn't sure if I was allowed to add new fields, so I didn't use this approach). In this approach, `update_keydb` would go over unprocessed transactions and aggregate their transaction value which would be used to update counters. I do think approach with counters is also faster, but has downside that you have to worry about distributed transactions. This approach that I have guarantees eventual consistency. 

`update_keydb` is triggered by a `BackgroundScheduler` on each full minute. It reads data from aggregator hash and writes it to corresponding key in a single keydb transaction.

## consumer.py

This is the main entrypoint to the program. It first tries to reconcile differences between existing data (seeded data) which is never received in RabbitMQ. It will load data from `historical_transactions` and available amount and set initial counters for all providers. This part would not be needed if table for historical transactions was empty.

After that it will start another process where cron task is scheduled to update keydb periodically. In this current process, it will start listening to incoming data from RabbitMQ. I've opted to start `update_keydb` task in another process because I've found that using two threads in one process (instead of one thread for two different processes) sometimes lead to desync in clock and task would be late 3-4 seconds (which is not allowed per task's constraints). I suspect this was due to GIL and starting time for task would be delayed because another thread which was waiting for RabbitMQ data was executing
