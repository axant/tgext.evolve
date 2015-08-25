#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging
from abc import abstractmethod
log = logging.getLogger('tgext.evolve')


class Evolver(object):
    def __init__(self, model, evolutions):
        super(Evolver, self).__init__()
        self._model = model

        def _alloc(evo):
            if isinstance(evo, Evolution):
                return evo
            return evo()
        self._evolutions = map(_alloc, evolutions)

    @abstractmethod
    def try_lock(self):
        pass

    @abstractmethod
    def unlock(self):
        pass

    @abstractmethod
    def is_locked(self):
        pass

    @abstractmethod
    def get_current_version(self):
        pass

    @abstractmethod
    def set_current_version(self, ver):
        pass

    def find_next_evolution(self, current_evolution_id):
        if current_evolution_id is None:
            return self._evolutions[0]

        for idx, ev in enumerate(self._evolutions):
            if ev.evolution_id == current_evolution_id:
                break
        else:
            return None

        try:
            return self._evolutions[idx+1]
        except IndexError:
            return None

    def evolve(self):
        if self.try_lock():
            curversion = self.get_current_version()
            log.info('Process %s, running evolutions after %s', os.getpid(), curversion)
            try:
                evolution = self.find_next_evolution(curversion)
                while evolution:
                    log.info('+ Running Evolution %s', evolution.evolution_id)
                    evolution.evolve()
                    log.info('- DONE!')
                    self.set_current_version(evolution.evolution_id)
                    evolution = self.find_next_evolution(evolution.evolution_id)
            finally:
                self.unlock()
                log.info('Evolutions Completed')
        else:
            log.info('Evolutions already undergoing, skipping...')


class Evolution(object):
    evolution_id = None

    def __init__(self):
        super(Evolution, self).__init__()
        if not self.evolution_id:
            raise ValueError('Evolutions must provide an "evolution_id" attribute')

    @abstractmethod
    def evolve(self):
        pass
