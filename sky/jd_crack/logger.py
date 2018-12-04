#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging


class ColoredStreamHandler(logging.StreamHandler):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = ['\033[0;{}m'.format(i) for i in range(30, 38)]
    COLOR_CYLE = [GREEN, YELLOW, BLUE, MAGENTA, CYAN]
    RESET = '\033[0m'

    def __init__(self, spider_id=None):
        logging.StreamHandler.__init__(self)
        self.spider_id = spider_id

    def format(self, record):
        spider_id = record.process if self.spider_id is None else self.spider_id

        datefmt = '%m-%d %H:%M:%S'
        COLOR = self.COLOR_CYLE[record.process % len(self.COLOR_CYLE)]

        if record.levelno == 10:
            FORMAT = '{}%(asctime)s [{} %(module)s_%(lineno)d] |{} {}%(message)s{}'. \
                format(COLOR, spider_id, self.RESET, '', '')
        elif record.levelno == 20:
            FORMAT = '{}%(asctime)s [{} %(module)s_%(lineno)d] |{} {}%(message)s{}'. \
                format(COLOR, spider_id, self.RESET, self.GREEN, self.RESET)
        else:
            FORMAT = '{}%(asctime)s [{} %(module)s_%(lineno)d] |{} {}%(message)s{}'. \
                format(COLOR, spider_id, self.RESET, self.RED, self.RESET)

        fmt = logging.Formatter(fmt=FORMAT, datefmt=datefmt)
        return fmt.format(record)

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg)
            stream.write(self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


class SHlogger(object):
    """自定义logger"""
    def __init__(self, spider_id=None, level=logging.DEBUG):
        self.logger = logging.getLogger('{}'.format(spider_id))
        self.logger.setLevel(level)
        self.sh = ColoredStreamHandler(spider_id)
        self.sh.setLevel(level)
        self.logger.addHandler(self.sh)


if __name__ == '__main__':
    #pass
    logger = SHlogger().logger
    logger.debug(1)
    logger.info(2)
    logger.warning(3)
