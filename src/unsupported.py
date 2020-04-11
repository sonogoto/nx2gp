#!/usr/bin/env python3


class Unsupported(Exception):
    pass


def unsupported(err_msg):
    def _unsupported(func):
        def _raise_exception(*args, **kwargs):
            raise Unsupported(err_msg)
        return _raise_exception

    return _unsupported


@unsupported("test unsupported")
def test_unsupported(arg1, arg2):
    pass


if __name__ == "__main__":
    test_unsupported(1, 2)
