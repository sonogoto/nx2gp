#!/usr/bin/env python3


class NotPermitted(Exception):
    pass


def not_permitted(err_msg):
    def _not_permitted(func):
        def _raise_exception(*args, **kwargs):
            raise NotPermitted(err_msg)
        return _raise_exception

    return _not_permitted


@not_permitted("test not_permitted")
def test_not_permitted(arg1, arg2):
    pass


if __name__ == "__main__":
    test_not_permitted(1, 2)
