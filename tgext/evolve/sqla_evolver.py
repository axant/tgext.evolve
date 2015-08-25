#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from .evolver import Evolver

log = logging.getLogger('tgext.evolve')


class SQLAEvolver(Evolver):
    def __init__(self, *args, **kwargs):
        super(SQLAEvolver, self).__init__(*args, **kwargs)
        raise NotImplementedError('Not yet available!')
