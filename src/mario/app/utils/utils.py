from datetime import datetime

def to_dict(keys, values):
    convert = {
        datetime: lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
    }
    def f(v):
        return convert.get(type(v), lambda x: x)(v)

    return dict(
        (k, f(v)) for (k, v) in zip(keys, values)
    )