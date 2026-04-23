import csv
import random
import time
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread

# Generate transactions in a csv file
def generate_transactions(filename="transactions.csv", n=1000000):
    currencies = ["USD", "EUR", "UAH"]
    categories = ["electronics", "food", "books", "clothes"]

    start_date = datetime(2025, 1, 1)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for _ in range(n):
            user_id = random.randint(1000, 9999)
            amount = round(random.uniform(10, 1000), 2)
            currency = random.choice(currencies)
            date = start_date + timedelta(days=random.randint(0, 100))
            category = random.choice(categories)

            writer.writerow([user_id, amount, currency, date.date(), category])

# Conversion rates for currencies
rates = {
    "USD": 1,
    "EUR": 1.1,
    "UAH": 0.025
}

# Convert transaction to dollar equivalent
def convert_currency(transaction):
    user_id, amount, currency, date, category = transaction
    amount = float(amount) * rates[currency]
    return [int(user_id), amount, currency, date, category]

# Apply cashback to users with certain ID
def apply_cashback(transaction):
    user_id, amount, currency, date, category = transaction
    if user_id > 8000:
        amount *= 0.8
    return [user_id, amount, currency, date, category]

# Pipeline processor
def pipeline_processing(filename="transactions.csv"):
    total = 0

    with open(filename, encoding="utf-8") as f:
        reader = csv.reader(f)

        for transaction in reader:
            transaction = convert_currency(transaction)
            transaction = apply_cashback(transaction)
            total += transaction[1]

    return total

# Producer function
def producer(filename, queue):
    with open(filename, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            queue.put(row)

    queue.put(None)

# Consumer function
def consumer(queue, result):
    total = 0

    while True:
        transaction = queue.get()

        if transaction is None:
            break

        transaction = convert_currency(transaction)
        transaction = apply_cashback(transaction)
        total += transaction[1]

    result.append(total)

# Function, which coordinates producer-consumer process
def producer_consumer_processing(filename="transactions.csv"):
    queue = Queue(maxsize=1000)
    result = []

    p = Thread(target=producer, args=(filename, queue))
    c = Thread(target=consumer, args=(queue, result))

    p.start()
    c.start()

    p.join()
    c.join()

    return result[0]

# Calculate time of execution
def benchmark(func):
    start = time.time()
    result = func()
    end = time.time()
    return result, end - start

if __name__ == "__main__":
    generate_transactions()

    pipeline_result, pipeline_time = benchmark(pipeline_processing)
    pc_result, pc_time = benchmark(producer_consumer_processing)

    print("Pipeline:", pipeline_result, pipeline_time)
    print("Producer-Consumer:", pc_result, pc_time)

""" RESULTS:
At n=1000000 producer-consumer method is over 3 times more productive, with the same result, 
because reading transactions and processing them are performed concurrently, which reduces idle 
time and improves resource utilization.
In addition, the Producer-Consumer pattern provides better scalability for streaming data, 
while the Pipeline pattern offers clearer stage separation, easier maintenance, and simpler extension 
of processing steps.
"""