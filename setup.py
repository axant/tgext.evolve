from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
try:
    README = open(os.path.join(here, 'README.rst')).read()
except IOError:
    README = ''

version = "0.0.3"

setup(
    name='tgext.evolve',
    version=version,
    description="Manages automatic updates and evolutions which are better suited outside schema migrations",
    long_description=README,
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='turbogears2.extension',
    author='AXANT',
    author_email='tech@axant.it',
    url='https://github.com/axant/tgext.evolve',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages = ['tgext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "TurboGears2 >= 2.3.6",
    ],
    entry_points="""
    # -*- Entry points: -*-
    """
)
