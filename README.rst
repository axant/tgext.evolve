About tgext.evolve
==================

tgext.evolve is a TurboGears2 extension for generic migrations and evolutions.

tgext.evolve *is safe to be used in multiprocessing and multithreading environments*
as it relies on a distributed lock on your database to perform evolutions.

During the evolution period the application will respond with a **503** status code,
feel free to configure TurboGears to trap it and use your ErrorController to provide
a custom page.

Installing
----------

tgext.evolve can be installed from pypi::

    pip install tgext.evolve

should just work for most of the users.

Enabling
--------

To enable tgext.evolve put inside your application
``config/app_cfg.py`` the following::

    import tgext.evolve
    tgext.evolve.plugme(base_config, options={
        'evolutions': [
            # ... Your evolutions ...
        ]
    })

or you can use ``tgext.pluggable`` when available::

    from tgext.pluggable import plug
    plug(base_config, 'tgext.evolve', evolutions=[
        # ... Your evolutions ...
    ])

tgext.evolve can then be disabled using the ``tgext.evolve.enabled = false``
option in your configuration files. This is often the case during test suites.

Using Evolutions
----------------

Evolutions are a subclass of ``tgext.evolve.Evolution`` that
must provide an ``evolution_id`` property and an ``evolve()``
method.

Here is a sample evolution that does not much apart waiting for 10 seconds::

    import time
    from tgext.evolve import Evolution

    class TestEvolution1(Evolution):
        evolution_id = 'test_evolution_1'

        def evolve(self):
            time.sleep(10)

Please note that in case you modify data or touch the database
through ``model.DBSession`` or other manners you must ``flush``
and ``commit`` your UnitOfWork and Transaction yourself as evolutions
are performed outside the transaction manager in a separated thread.

Then you can register your evolution with something like::

    from tgext.pluggable import plug
    plug(base_config, 'tgext.evolve', evolutions=[
        TestEvolution1
    ])

In case your evolution requires parameters you can register an
instance of it instead of registering the class.

.. note::

    There is no opposite to the ``evolve()`` method as there is no
    guarantee that evolutions are reversible. In case you need
    reversible migrations for database schemas please use a
    schema migration framework.

ChangeLog
---------

0.0.3
~~~~~

* Experimental support for SQLAlchemy, tested on SQLite and MySQL.

0.0.2
~~~~~

* Allow disabling tgext.evolve using ``tgext.evolve.enabled = False`` in *.ini files.

0.0.1
~~~~~

* First release with only MongoDB/Ming support.
