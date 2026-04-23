import os
import time
import random
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
from pathlib import Path

# Generate input files randomly
def generate_html_files(n=1000, folder="html_data"):
    Path(folder).mkdir(exist_ok=True)

    tags = ['html', 'body', 'div', 'p', 'a', 'span', 'ul', 'li']

    for i in range(n):
        content = ""
        for _ in range(random.randint(100, 500)):
            tag = random.choice(tags)
            content += f"<{tag}>text</{tag}>"

        with open(f"{folder}/file_{i}.html", "w", encoding="utf-8") as f:
            f.write(content)

# Count tags in a given file
def count_tags_in_file(filepath):
    with open(filepath, encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    counts = {}
    for tag in soup.find_all():
        counts[tag.name] = counts.get(tag.name, 0) + 1
    return counts

# Sequentially scan every file in the folder
def sequential_tag_count(folder):
    total = {}
    for file in os.listdir(folder):
        counts = count_tags_in_file(os.path.join(folder, file))
        for tag, count in counts.items():
            total[tag] = total.get(tag, 0) + count
    return total

# Map the files in the folder into multiple tasks, then reduce into one result
def map_reduce_tag_count(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder)]

    with Pool(cpu_count()) as pool:
        mapped = pool.map(count_tags_in_file, files)

    reduced = {}
    for counts in mapped:
        for tag, count in counts.items():
            reduced[tag] = reduced.get(tag, 0) + count
    return reduced

# Create a pool of workers which scan files
def worker_pool_tag_count(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder)]

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(count_tags_in_file, files))

    total = {}
    for counts in results:
        for tag, count in counts.items():
            total[tag] = total.get(tag, 0) + count
    return total

# Scan folder and pass to main function
def fork_join(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder)]
    return fork_join_tag_count(files)

# Divide the files recursively, and calculate the result at the lowest level
def fork_join_tag_count(files):
    if len(files) == 1:
        return count_tags_in_file(files[0])

    mid = len(files) // 2
    left = fork_join_tag_count(files[:mid])
    right = fork_join_tag_count(files[mid:])

    for tag, count in right.items():
        left[tag] = left.get(tag, 0) + count
    return left

if __name__ == "__main__":

    #print("Generating hmtl files...")
    #generate_html_files()

    print("Done generating files")

    print("Sequential:")
    start = time.time()
    r1 = sequential_tag_count("html_data")
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r1}")

    print("Map reduce")
    start = time.time()
    r2 = map_reduce_tag_count("html_data")
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r2}")

    print("Worker Pool")
    start = time.time()
    r3 = worker_pool_tag_count("html_data")
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r3}")

    print("Fork Join")
    start = time.time()
    r4 = fork_join("html_data")
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r4}")

""" RESULTS:
Both map reduce and worker pool are significantly faster than the sequential read.
Fork join was slightly faster than the sequential method.
"""