import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Pool, cpu_count
import numpy as np

# Normal multiplication (already has built-in parallelization)
def normal_matrix_multiply(A, B):
    return np.dot(A, B)

# Simple multiplication (sequential)
def naive_matrix_multiply(A, B):
    n = len(A)
    m = len(B[0])
    p = len(B)

    result = np.zeros((n, m))

    for i in range(n):
        if i % 100 == 0:
            print(i)
        for j in range(m):
            for k in range(p):
                result[i][j] += A[i][k] * B[k][j]

    return result

# Map reduce
def map_reduce_matrix(A, B):
    with Pool(cpu_count()) as pool:
        rows = pool.map(multiply_row, [(row, B) for row in A])
    return np.array(rows)

# Multiply one row
def multiply_row(args):
    row, B = args
    return np.dot(row, B)

# Worker pool
def worker_pool_matrix(A, B):
    with ProcessPoolExecutor() as executor:
        rows = list(executor.map(multiply_row, [(row, B) for row in A]))
    return np.array(rows)

# Fork join
def fork_join_matrix(A, B):
    if len(A) == 1:
        return np.dot(A, B)

    mid = len(A)//2
    top = fork_join_matrix(A[:mid], B)
    bottom = fork_join_matrix(A[mid:], B)
    return np.vstack((top, bottom))

if __name__ == "__main__":

    print("Generating input matrices...")
    A = np.random.rand(1000, 1000)
    B = np.random.rand(1000, 1000)
    print("Done generating matrices")

    print("Normal:")
    start = time.time()
    r1 = normal_matrix_multiply(A, B)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r1}")

    print("Map reduce")
    start = time.time()
    r2 = map_reduce_matrix(A, B)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r2}")

    print("Worker Pool")
    start = time.time()
    r3 = worker_pool_matrix(A, B)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r3}")

    print("Fork Join")
    start = time.time()
    r4 = fork_join_matrix(A, B)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r4}")

    print("Unoptimized:")
    start = time.time()
    r5 = naive_matrix_multiply(A, B)
    print(f"Time Elapsed: {time.time() - start}")
    print(f"Result: {r5}")

""" RESULTS:
Fork join has the best execution time, and is preferred in this scenario. This is because matrix multiplication benefits
greatly from recursive splitting of multiplication.
Method np.dot() already has optimizations built-in, which makes it on-par with the fork-join implementation.
Second place goes to map reduce method, then Worker pool, both much slower than the best method.
At 1000 by 1000 elements naive multiplication takes more than 5 minutes to complete, making it extremely slow.
"""
