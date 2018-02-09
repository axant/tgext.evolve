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
        """returns False if the lock is already set
           returns True if a new lock is created and has the same pid"""
        from pymongo.errors import DuplicateKeyError

        col = self._col
        col.ensure_index('type', background=False, unique=True)

        pid = os.getpid()
        try:
            distlock = col.find_and_modify({'type': 'lock', 'process': None},
                                           update={'type': 'lock', 'process': pid},
                                           new=True,
                                           upsert=True)
            log.info('last lock was correctely released')
            return True
        except DuplicateKeyError:
            lock = col.find_one({'type': 'lock'})
            try:
                os.kill(lock['process'], 0)
                log.info('the running process %s holds the lock' % lock['process'])
                return False  # the process is still running
            except OSError:
                log.warning('last lock was not released correctely!')
                col.find_and_modify({'type': 'lock', 'process': lock['process']},
                                    update={'type': 'lock', 'process': pid})
                return True

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
