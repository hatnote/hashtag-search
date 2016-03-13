# -*- coding: utf-8 -*-

def encode_vals(indict):
    ret = {}
    for k, v in indict.items():
        if isinstance(v, unicode):
            ret[k] = v.encode('utf8')
        else:
            ret[k] = v
    return ret


def to_unicode(obj):
    try:
        return unicode(obj)
    except UnicodeDecodeError:
        return unicode(obj, encoding='utf8')
