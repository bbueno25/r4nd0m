import random as sys_random
from Crypto.Random import random as crypto_random
from numpy         import random as np_random


class Generators:

    def __init__(self, length):
        self.length = length

    def crypto_integer(self, low=-250, high=250):
        nrng = []
        for _ in range(self.length):
            nrng.append(crypto_random.randint(low, high))
        return nrng

    def numpy_float(self):
        return np_random.uniform(low=0.0, high=1.0, size=self.length)                              # pylint: disable=E1101

    def numpy_integer(self):
        return np_random.randint(low=-250, high=250, size=self.length)                             # pylint: disable=E1101

    def system_integer(self, low=-250, high=250):
        rng = sys_random.SystemRandom()
        nrng = []
        for _ in range(self.length):
            nrng.append(rng.randint(low, high))
        return nrng
