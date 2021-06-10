import time

def retry(times=3, delay=1, exceptions=Exception, logger=None):
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    message = (
                        "Retry exception thrown when attempting to run {}, "
                        "attempt {} of {}".format(func, attempt, times)
                    )
                    if logger:
                        logger.info(message)
                    else:
                        print(message)
                    attempt += 1
                    time.sleep(delay)
            return func(*args, **kwargs)

        return newfn

    return decorator