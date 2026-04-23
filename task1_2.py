import time
import statistics
import numpy as np
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor


# Sequential calculation
def stats_sequential(arr):
    return min(arr), max(arr), statistics.median(arr), sum(arr)/len(arr)

# Function for chunks of result
def chunk_stats(chunk):
    return min(chunk), max(chunk), sum(chunk), len(chunk)

# Map reduce approach
def stats_map_reduce(arr, chunks=8):
    split = np.array_split(arr, chunks)

    with Pool(chunks) as pool:
        results = pool.map(chunk_stats, split)

    mins, maxs, sums, lens = zip(*results)
    return min(mins), max(maxs), statistics.median(arr), sum(sums)/sum(lens)

# Worker pool approach
def stats_worker_pool(arr, chunks=8):
    split = np.array_split(arr, chunks)

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(chunk_stats, split))

    mins, maxs, sums, lens = zip(*results)
    return min(mins), max(maxs), statistics.median(arr), sum(sums)/sum(lens)

# Fork join approach
def fork_join(arr):
    if len(arr) < 100000:
        return min(arr), max(arr), sum(arr), len(arr)

    mid = len(arr) // 2

    left = fork_join(arr[:mid])
    right = fork_join(arr[mid:])

    return (
        min(left[0], right[0]),
        max(left[1], right[1]),
        left[2] + right[2],
        left[3] + right[3]
    )

# Get results and calculate mean
def stats_fork_join(arr):
    min_val, max_val, total_sum, total_count = fork_join(arr)
    median_val = statistics.median(arr)
    mean_val = total_sum / total_count

    return min_val, max_val, median_val, mean_val

if __name__ == "__main__":

    print("Generating input array...")
    array = np.random.exponential(scale=10, size=10_000_000)
    print("Done generating array")

    print("Sequential:")
    start = time.time()
    r1 = stats_sequential(array)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r1}")

    print("Map reduce")
    start = time.time()
    r2 = stats_map_reduce(array)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r2}")

    print("Worker Pool")
    start = time.time()
    r3 = stats_worker_pool(array)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r3}")

    print("Fork Join")
    start = time.time()
    r4 = stats_fork_join(array)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r4}")

""" RESULTS:
Best time by worker pool, followed by map reduce. Fork join did not improve the base time of sequential calculation.
This makes sense, as fork join does basically the same thing, as sequential method, but with some additional 
overhang.
"""