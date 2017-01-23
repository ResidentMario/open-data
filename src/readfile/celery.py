from celery import Celery

app = Celery('readfile',
             broker='redis://localhost',
             include=['readfile.tasks'])

if __name__ == '__main__':
    app.start()
