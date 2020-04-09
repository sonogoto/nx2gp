#!/usr/bin/env python3


class Unsupported(Exception):
    pass


def unsupported(err_msg):
    def _unsupported(func):
        def _raise_exception():
            raise Unsupported(err_msg)
        return _raise_exception

    return _unsupported

