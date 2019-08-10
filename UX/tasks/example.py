from UX.extensions import celery


@celery.task
def dummy_task():
    print("XX")
    return "OK"
