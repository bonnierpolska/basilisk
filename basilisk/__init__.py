"""
Basilisk enables Pythonic use of Redis hashes, lists and sorted sets with simple class interface as well as provides
an ORM Model-like class using Redis hash or Elasticsearch inside.

A simple example:

>>> import basilisk
>>>
>>> basilisk.Config.load(redis={'host': 'localhost',
>>>                             'port': 6379,
>>>                             'db': 0,
>>>                             'max_connections': 10},
>>>                      elastic={})
>>>
>>>
>>> class Item(basilisk.RedisModel):
>>>     id = basilisk.MapField(key=True)
>>>     name = basilisk.MapField()
>>>     content = basilisk.MapField()
>>>     attachments = basilisk.JsonMapField()
>>>
>>>     @classmethod
>>>     def select(cls):
>>>         redis_items = basilisk.RedisList('items')
>>>         variables = basilisk.RedisHash('items_variables')
>>>         last_modified = int(variables['last_modified'] or 0)
>>>
>>>         if (not len(redis_items) or
>>>             not last_modified or
>>>             last_modified + 30 < time.time()):
>>>             items = DownloadNewItemsFromDatabase(last_modified)
>>>             for item in items:
>>>                 redis_items.append(item.id)
>>>             variables['last_modified'] = int(time.time())
>>>             variables.save()
>>>         return list(redis_items)
>>>
>>> items = Item.select()
>>> for item in items:
>>>     print(Item.get(item).content)

"""
from .fields import MapField, JsonMapField
from .redis_entities import RedisModel, RedisList, RedisHash, RedisSortedSet, RedisModelException
from .elasticsearch_entities import ElasticsearchModelException, ElasticsearchModel
from .base import Config, MapModelBase
