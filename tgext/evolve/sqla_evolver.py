#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
from sqlalchemy import MetaData, Table, Column, String, Integer
from sqlalchemy.exc import IntegrityError
from tg.caching import cached_property
from .evolver import Evolver

log = logging.getLogger('tgext.evolve')


class SQLAEvolver(Evolver):
    def __init__(self, *args, **kwargs):
        super(SQLAEvolver, self).__init__(*args, **kwargs)

    @cached_property
    def _engine(self):
        return self._model.DBSession.bind

    @cached_property
    def _metadata(self):
        return MetaData(bind=self._engine)

    @cached_property
    def _table(self):
        t = Table(
            'tgext_evolve',
            self._metadata,
            Column('type', String(16), primary_key=True),
            Column('value', String(255), nullable=False)
        )
        if not t.exists():
            t.create()
        return t

    def try_lock(self):
        try:
            self._engine.execute(self._table.insert().values(type='lock', value=os.getpid()))
        except IntegrityError:
            return False
        else:
            return True

    def unlock(self):
        self._engine.execute(self._table.delete().where(
            (self._table.c.type == 'lock') & (self._table.c.value == os.getpid())
        ))

    def is_locked(self):
        lock = self._engine.execute(self._table.select().where(self._table.c.type == 'lock'))
        lock = lock.fetchall()
        if len(lock) != 0:
            return True
        return False

    def get_current_version(self):
        verinfo = self._engine.execute(self._table.select().where(self._table.c.type == 'version'))
        verinfo = verinfo.first()
        if not verinfo:
            return None
        return verinfo.value

    def set_current_version(self, ver):
        try:
            self._engine.execute(self._table.insert().values(type='version', value=ver))
        except IntegrityError:
            self._engine.execute(self._table.update().where(self._table.c.type == 'version').values(value=ver))
