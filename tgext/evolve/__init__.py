from tg.configuration import milestones
from tg.appwrappers.base import ApplicationWrapper
from evolver import Evolution
from webob.exc import HTTPServiceUnavailable
from threading import Thread

__all__ = ['plugme', 'Evolution']

import logging
log = logging.getLogger('tgext.evolve')


def plugme(configurator, options=None):
    if options is None:
        options = {}

    evolutions = options.get('evolutions')
    if not evolutions:
        raise ValueError('"evolutions" option is required and must be a list of tgext.evolve.Evolution subclasses')

    log.info('Setting up tgext.evolve extension...')
    milestones.config_ready.register(_SetupExtension(configurator, evolutions))

    # This is required to be compatible with the
    # tgext.pluggable interface
    return dict(appid='tgext.evolve')


class _SetupExtension(object):
    def __init__(self, configurator, evolutions):
        self._configurator = configurator
        self._evolutions = evolutions

    def __call__(self):
        from tg import hooks
        hooks.register('configure_new_app', self.on_app_configured)
        self._configurator.register_wrapper(_MaintenanceApplicationWrapper)

    def on_app_configured(self, app):
        config = app.config

        enabled = config.get('tgext.evolve.enabled', 'True').lower() == 'true'
        log.info('tgext.evolve enabled: %s', enabled)
        if not enabled:
            return

        model = config['package'].model

        if config.get('use_sqlalchemy', False):
            log.info('Configuring tgext.evolve for SQLAlchemy')
            from .sqla_evolver import SQLAEvolver
            config['tgext.evolve._evolver'] = SQLAEvolver(model, self._evolutions)
        elif config.get('use_ming', False):
            log.info('Configuring tgext.evolve for Ming')
            from .ming_evolver import MingEvolver
            config['tgext.evolve._evolver'] = MingEvolver(model, self._evolutions)
        else:
            raise ValueError('tgext.evolve should be used with sqlalchemy or ming')

        evolver = config['tgext.evolve._evolver']
        evolution_thread = Thread(target=lambda *args, **kwargs: evolver.evolve())
        evolution_thread.daemon = True
        evolution_thread.start()


class _MaintenanceApplicationWrapper(ApplicationWrapper):
    def __init__(self, handler, config):
        super(_MaintenanceApplicationWrapper, self).__init__(handler, config)
        self._should_check = True

    def __call__(self, controller, environ, context):
        if not self._should_check:
            return self.next_handler(controller, environ, context)

        if self._should_check:
            evolver = context.config.get('tgext.evolve._evolver', None)
            if evolver is None or not evolver.is_locked():
                self._should_check = False
                return self.next_handler(controller, environ, context)

            return HTTPServiceUnavailable(detail='System is currently undergoing maintenance')
