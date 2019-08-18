# This is the work to reproduce the model published in this paper
# Performance modelling and cost effective execution for distributed graph processing on configurable VMs, CCGrid'17
from scipy.optimize import least_squares
import math
import numpy as np
from sklearn.metrics import mean_squared_error

def communication_time(p1, p2, q1, M, B):
    Einter = (E*(M-1))/(M*M)
    return (Einter * p1 * p2 * q1)/B

def computation_time(p1, p3, p4, q1, M):
    return (N/M)*p3 + (E/M)*q1 * p1 * p4

def synchronization_time(q2, q3, M):
    return q2 * math.pow(M, q3)

def execution_time(x, M, B):
    p1, p2, p3, p4, q1, q2, q3, kcomp, kcomm  = x

    return kcomm * communication_time(p1, p2, q1, M, B) \
        + kcomp * computation_time(p1, p3, p4, q1, M) + synchronization_time(q2, q3, M)

def fun(x, M, B , Y):
    # print np.array([execution_time(x, m , b) - y for b, m, y in zip(B, M, Y)])
    return np.array([execution_time(x, m , b) - y for b, m, y in zip(B, M, Y)])

N = 10000
E = 100000

B = range(100, 1000, 100)
M = range(1, 10)
print(B)
print(M)
x1 = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3]
y = [ execution_time(x1, m, b) for b, m in zip(B,M)]
print(y)
x0 = np.ones(9)
res_lsq = least_squares(fun, x0, args=(M, B , y))
print x1
print res_lsq.x

y_pred = [ execution_time(res_lsq.x, m, b) for b, m in zip(B,M)]

print mean_squared_error(y, y_pred)
