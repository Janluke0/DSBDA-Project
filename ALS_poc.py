import numpy as np
import numba
import functools
import timeit


def parse_csv():
    import pandas as pd
    df = pd.read_csv("ratings_small.csv")
    df = df[df.columns[:3]]
    df = df.pivot(index="userId", columns="movieId", values="rating")
    R = df.values
    W = (~np.isnan(R)).astype("int")
    np.save("R.npy", R)
    np.save("W.npy", W)


iterations, factors, _lambda = 200, 15, 0.01
real_data = True
if real_data:
    R = np.load("R.npy")[:20, :200]
    W = np.load("W.npy")[:20, :200].astype("float")
    R[~W.astype("bool")] = 0.
    users, items = R.shape
else:
    np.random.seed(42)
    users, items = 300, 2000
    R = np.random.randint(0, 11, (users, items))/2
    W = np.random.randint(0, 2, (users, items)).astype("float")
U = np.random.rand(users, factors)
P = np.random.rand(factors, items)


L = _lambda*np.eye(factors)

@numba.njit(parallel=True)
def als_it(U=U,P=P,R=R,W=W):
    for i in numba.prange(users):
        U[i] = np.linalg.solve(P @ np.diag(W[i]) @ P.T + L,
                               P @  np.diag(W[i]) @ R[i])

    for j in numba.prange(items):
        P[:, j] = np.linalg.solve(U.T  @ np.diag(W[:, j]) @ U + L,
                                  U.T @ np.diag(W[:, j]) @ R[:, j])

def als_it_no_nu():
    for i in range(users):
        U[i] = np.linalg.solve(P @ np.diag(W[i]) @ P.T + L,
                               P @  np.diag(W[i]) @ R[i])

    for j in range(items):
        P[:, j] = np.linalg.solve(U.T  @ np.diag(W[:, j]) @ U + L,
                                  U.T @ np.diag(W[:, j]) @ R[:, j])

#print(timeit.timeit(als_it, number=500))
#print(timeit.timeit(als_it_no_nu, number=500))
#exit()

for it in range(iterations):
    als_it()
    if it % 3 == 0:
        print(np.sqrt((R - U@P)**2).mean())

print(R)
print(U@P)
