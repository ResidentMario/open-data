from .celery import app


@app.task(time_limit=1)
def add(x, y):
    import time
    time.sleep(5)
    return x + y


@app.task
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)
