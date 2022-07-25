def chain_func(start, *funcs):
    res = start
    for func in funcs:
        res = func(res)
    return res


def listify(x) -> list:
    """Ensure list type. Create a copy."""
    if isinstance(x, (str, bytes)):
        return [x]
    try:
        return [_ for _ in x]
    except TypeError:
        return [x]
