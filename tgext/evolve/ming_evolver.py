#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
from .evolver import Evolver

log = logging.getLogger('tgext.evolve')


class MingEvolver(Evolver):
    def _get_session(self):
        return self._model.DBSession._get().impl

    @property
    def _col(self):
        return self._get_session().db.tgext_evolve

    def try_lock(self):
        from pymongo.errors import DuplicateKeyError

        col = self._col
        col.ensure_index('type', background=False, unique=True)

        pid = os.getpid()
        try:
            distlock = col.find_and_modify({'type': 'lock', 'process': None},
                                           update={'type': 'lock', 'process': pid},
                                           new=True,
                                           upsert=True)
        except DuplicateKeyError:
            return False
        else:
            return distlock['process'] == pid

    def unlock(self):
        self._col.find_and_modify({'type': 'lock', 'process': os.getpid()},
                                  update={'type': 'lock', 'process': None},
                                  new=True)

    def is_locked(self):
        lock = self._col.find_one({'type': 'lock'})
        if lock is None:
            return False
        return lock['process'] is not None

    def get_current_version(self):
        verinfo = self._col.find_one({'type': 'version'})
        if not verinfo:
            return None
        return verinfo.get('current')

    def set_current_version(self, ver):
        self._col.update({'type': 'version'}, {'type': 'version', 'current': ver}, upsert=True)