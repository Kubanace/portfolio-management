import array

import numpy as np
import time


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print('{} with {} rows: {elapse:2.2f} sec'.format(method.__name__, args[0], elapse=te-ts))
        return result
    return timed


@timeit
def numpy_mat_prod(n_):
    _a = np.random.rand(n_, n_)
    _b = np.random.rand(n_, 1)
    return np.linalg.matmul(_a, _b)


@timeit
def loop_mat_prod(n_):
    _a = np.random.rand(n_, n_)
    _x = np.random.rand(n_, 1)
    _b = np.empty((n_, 1))

    for j in range(n_):
        for i in range(n_):
            _b[i] += _a[i, j]*_x[j]
    return _b


# n = 1000
# np_prod = numpy_mat_prod(n)
# loop_prod = loop_mat_prod(n)

# Cholesky decomposition
A = np.array([[1, 1, 1], [1, 2, 2], [1, 2, 3]])
R = np.linalg.cholesky(A, upper=True)
print(R)