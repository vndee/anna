from anna import timer

@timer
def add(a, b):
    return a + b

print(add(3, 5))