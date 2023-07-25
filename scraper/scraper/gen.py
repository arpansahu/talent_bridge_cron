def adder(i):
    return i + 1000

def generator():
    for i in range(1,6):
        yield adder(i)


for j in generator():
    print(j)