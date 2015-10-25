import logging
import logging.handlers
import sys
import os

def setup(toaddrs, partner_path):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(FORMAT)
    logger = logging.getLogger(os.path.realpath(__file__))
    logger.setLevel(logging.DEBUG)
    logger.propagate = 0
    
    # Log debug in file
    folder = os.path.join(partner_path, 'log/') 
    if not os.path.exists(folder): os.makedirs(folder)
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(folder, 'logging.out'), maxBytes=50000, backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Log exception to email
    mailhost = 'smtp.criteois.lan'
    fromaddr = 'f.pillot@criteo.com'
    subject = "Uncaught exception"
    mail_handler = logging.handlers.SMTPHandler(mailhost, fromaddr, toaddrs, subject, credentials=None)
    mail_handler.setLevel(logging.ERROR)
    mail_handler.setFormatter(formatter)
    logger.addHandler(mail_handler)
    
    # Log warning to stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)

    return logger

if __name__ == '__main__':
    logger = setup()
    try:
        logger.warning('This is a test warning')
        logger.info('This is a test info')
        logger.debug('This is to debug')
        MyException = []
        MyException[1]
    except Exception:
        logger.exception("Uncaught exception:")
