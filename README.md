Basilisk
========

[![Build Status](https://travis-ci.org/bonnierpolska/basilisk.svg)](https://travis-ci.org/bonnierpolska/basilisk)

Basilisk is a object-NoSQL mapper for Python 2.7 and 3.3+, supporting models, lists, hashes and sorted sets.

A simple example:

```python
import basilisk                                                
                                                               
basilisk.Config.load(redis={'host': 'localhost',
                            'port': 6379,
                            'db': 0,
                            'max_connections': 10},
                     elastic={})


class Item(basilisk.RedisModel):
    id = basilisk.MapField(key=True)
    name = basilisk.MapField()
    content = basilisk.MapField()
    attachments = basilisk.JsonMapField()

    @classmethod
    def select(cls):
        redis_items = basilisk.RedisList('items')
        variables = basilisk.RedisHash('items_variables')
        last_modified = int(variables['last_modified'] or 0)

        if (not len(redis_items) or
            not last_modified or
            last_modified + 30 < time.time()):
            items = DownloadNewItemsFromDatabase(last_modified)
            for item in items:
                redis_items.append(item.id)
            variables['last_modified'] = int(time.time())
            variables.save()
        return list(redis_items)

items = Item.select()
for item in items:
    print(Item.get(item).content)
```

Installation
------------
You can just `pip install basilisk`.

Documentation
-------------

Feel free to browse the code and especially the tests to see what's going on behind the scenes.
The current verson of Sphinx docs is always on http://basiliskpy.readthedocs.org/en/latest/

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
pylint basilisk --rcfile=.pylintrc
coverage run --source=basilisk -m basilisk.tests && coverage report -m
cd docs && make html
```