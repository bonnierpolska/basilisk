Basilisk
========

[![Build Status](https://travis-ci.org/bonnierpolska/basilisk.svg)](https://travis-ci.org/bonnierpolska/basilisk)

Basilisk is a object-NoSQL mapper for Python 2.7 and 3.3+, supporting models, lists, hashes and sorted sets.

Installation
------------

Documentation
-------------

Feel free to browse the code and especially the tests to see what's going on behind the scenes.

Questions and contact
---------------------

If you have any questions, feedback, want to say hi or talk about Python, just hit me up on
https://twitter.com/bujniewicz

Contributions
-------------

Please read CONTRIBUTORS file before submitting a pull request.

We use Travis CI. The targets are 10.00 for lint 10.00 and 100% for coverage, as well as building sphinx docs.

You can of also check the build manually, just make sure to `pip install -r requirements.txt` before:

```
pylint devourer --rcfile=.pylintrc
coverage run --source=basilisk -m basilisk.tests && coverage report -m
cd docs && make html
```