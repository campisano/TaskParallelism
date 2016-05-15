#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import pprint


def toString(
    _an_object,
    _indent=2,
    _width=1
):
    pp = pprint.PrettyPrinter(indent=_indent, width=_width)

    return pp.pformat(_an_object)
