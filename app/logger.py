import logging
import datetime

def setup_logger():
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.DEBUG)
    
    # create a file handler
    current_date = datetime.date.today()
    date = current_date.strftime("%d-%m-%Y")

    current_time = datetime.datetime.now().time()
    time = current_time.strftime("%H:%M:%S")

    handler = logging.FileHandler(f'app/logs/archival-logs-{date}-{time}).log')
    handler.setLevel(logging.DEBUG)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    return logger