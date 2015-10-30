"""
This module defines base classes corresponding to Redis types as well
as Redis model.
"""
from collections import defaultdict
from six import with_metaclass

from .base import RedisModelRegister, RedisModelCreator, MapModelBase, MapModelException

__all__ = ['RedisModel', 'RedisSortedSet', 'RedisModelException', 'RedisHash', 'RedisList']


class RedisModelException(MapModelException):
    """
    Exception raised when errors related to Redis handling are encountered.
    """
    pass


class RedisModel(with_metaclass(RedisModelCreator, MapModelBase)):
    """
    This is the base class for Redis models. Internally they are just a Redis hash.
    This class enables reading object with given id, saving object and data
    (de)serialization.
    Redis connection is available in connect property.
    Dict of fields is available in _fields property.

    Reserved property names, apart from methods, are _fields, id_field and connect.
    :type connect: redis.Redis
    """
    __metaclass__ = RedisModelCreator
    MapModelException = RedisModelException
    RedisModelException = RedisModelException

    namespace = 'redis'
    connect = None

    def save(self, create_id=True):
        """
        Let's save instance's current state to Redis.
        :param create_id: whether id should be created automatically if it's not set yet.
        :return: self
        """
        self._save(create_id)
        self.connect.hmset(self.get_instance_key(), self.serialize())
        return self

    @classmethod
    def get_key(cls, oid):
        """
        This function creates a key in which Redis will save the instance with given id.
        :param oid: id of object for which a key should be created.
        :return: Redis key.
        """
        return "{0.__module__}.{0.__name__}.{1}".format(cls, oid)

    @classmethod
    def get(cls, oid):
        """
        This method gets a model instance with given id from Redis.
        :param oid: id of object to get.
        :return: hydrated model instance.
        """
        data = cls.connect.hgetall(cls.get_key(oid))
        if data:
            return cls(**cls.pythonize(data))
        raise RedisModelException('No object with primary key {} of class {}'.format(cls.get_key(oid), cls.__name__))


class RedisSortedSetSlice(object):
    """
    An inner class proxying ranges returned by ZRANGEBYSCORE to enable indexing by count.
    It does not enable changing elements' values.
    :type connect: redis.Redis
    """

    def __init__(self, connect, key, start, end):
        """
        This method sets up the properties required by object to work.
        :param connect: Redis connection.
        :param key: key where sorted set is kept.
        :param start: starting SCORE
        :param end: ending SCORE
        :return:
        """
        self.connect = connect
        self.key = key
        self.start = start or '-inf'
        self.end = end or '+inf'

    def __getitem__(self, item):
        """
        This function translates Python index and slice into ZRANGEBYSCORE and returns Redis's response.
        :param item: index or slice to get.
        :return: element or a list of elements.
        """
        if isinstance(item, slice):
            if item.start is None:
                start = 0
            else:
                start = item.start
            if item.stop is None:
                return self.connect.zrangebyscore(self.key, self.start, self.end, start, len(self))
            return self.connect.zrangebyscore(self.key, self.start, self.end, start, item.stop-item.start)
        else:
            return self.connect.zrangebyscore(self.key, self.start, self.end, item, 1)[0]

    def __len__(self):
        """
        Returns Redis-counted number of elements in range.
        :return: number of elements in range.
        """
        return self.connect.zcount(self.key, self.start, self.end)


class RedisSortedSet(object):
    """
    This class is used to proxy Redis's Sorted Sets. It allows value search with
    pagination and delayed (lazy) key alterations. Indexing works with SCORE,
    not MEMBER or RANK.
    set_score and delete_item methods don't interface with Redis directly, but are
    queued in a change list.
    :type connect: redis.Redis
    :type changes: dict
    """
    namespace='redis'

    def __init__(self, name, namespace=None):
        """
        This function creates changelist and remembers the name of this sorted set.
        By default name is used as Redis key for this instance.

        :param namespace: name of connection used for this instance.
        :param name: name of sorted set.
        :return:
        """
        self.connect = RedisModelRegister(namespace or self.namespace).connect()
        self.name = name
        self.changes = defaultdict(list)

    def clear(self):
        """
        I'm tired of you, off you go. Disappear from Redis. NOW.
        :return:
        """
        self.connect.delete(self.get_instance_key())

    def __getitem__(self, item):
        """
        Returns RedisSortedSetSlice for given SCORE or its range passed as a slice.
        :param item: SCORE or slice [SCORE MIN, SCORE MAX].
        :return: RedisSortedSetSlice for given SCORE or its range.
        """
        if isinstance(item, slice):
            return RedisSortedSetSlice(self.connect, self.get_instance_key(), item.start, item.stop)
        return RedisSortedSetSlice(self.connect, self.get_instance_key(), item, item)

    def __delitem__(self, item):
        """
        Removes elements with given SCORE or in given SCORE range.
        :param item: SCORE or slice [SCORE MIN, SCORE MAX]
        """
        if isinstance(item, slice):
            start = item.start or '-inf'
            stop = item.stop or '+inf'
            self.connect.zremrangebyscore(self.get_instance_key(), start, stop)
        else:
            self.connect.zremrangebyscore(self.get_instance_key(), item, item)

    def __len__(self):
        """
        Let's see how many items do we have in our set. As returned by Redis.
        :return: number of elements in set.
        """
        return self.connect.zcard(self.get_instance_key())

    def set_score(self, item, score):
        """
        This function adds a new element if it's not in Redis and sets its SCORE.
        You need to call save() to propagate changes to Redis.
        :param item: element to be added or modified.
        :param score: element's SCORE.
        :return:
        """
        self.changes[item].append(float(score))

    def delete_item(self, item):
        """
        This method deletes given element.
        You need to call save() to propagate changes to Redis.
        :param item: element to be removed.
        :return:
        """
        self.changes[item].append(None)

    def lowest(self):
        """
        Returns element with lowest SCORE and its SCORE.
        :return: element with lowest SCORE and its SCORE.
        """
        return (self.connect.zrange(self.get_instance_key(), 0, 0, withscores=True) or [(None, 0)])[0]

    def highest(self):
        """
        Returns element with highest SCORE and its SCORE.
        :return: element with highest SCORE and its SCORE.
        """
        return (self.connect.zrevrange(self.get_instance_key(), 0, 0, withscores=True)or [(None, 0)])[0]

    def save(self):
        """
        This method analyzes changelist and using as few operations as possible propagates
        changes to Redis's Sorted Set representing this instance.
        :return:
        """
        to_remove = []
        to_add = {}
        for key, value in self.changes.items():
            if value[-1] is None:
                to_remove.append(key)
            else:
                to_add[key] = value[-1]
        if to_remove:
            self.connect.zrem(self.get_instance_key(), *to_remove)
        if to_add:
            self.connect.zadd(self.get_instance_key(), **to_add)
        self.changes.clear()

    def get_instance_key(self):
        """
        This function creates Redis's instance key.
        :return: key in which instance will be saved.
        """
        return self.get_key(self.name)

    @classmethod
    def get_key(cls, name):
        """
        This method creates a Redis key in which instance with given name will be saved.
        :param name: name of object for which a key is to be made.
        :return: key used in Redis for given name.
        """
        return name


class RedisHash(object):
    """
    This class acts as a proxy for Redis Hash. It enables delayed modifications.
    __setitem__ and __delitem__ methods don't modify Redis immediately, but are instead
    ququed in a changelist.
    :type connect: redis.Redis
    :type changes: dict
    """
    namespace = 'redis'

    def __init__(self, name, namespace=None):
        """
        This function initializes changelist and remembers name of the hash.
        By default name is used as Redis key for this instance.
        :param namespace: name of connection used by this instance.
        :param name: name of the hash.
        :return:
        """
        self.connect = RedisModelRegister(namespace or self.namespace).connect()
        self.name = name
        self.changes = defaultdict(list)

    def clear(self):
        """
        This removes whole hash from Redis.
        :return:
        """
        self.connect.delete(self.get_instance_key())

    def get(self, *fields):
        """
        This gets one or many items from Redis.
        :param fields: list of fields to get.
        :return:
        """
        return self.connect.hmget(self.get_instance_key(), *fields)

    def __getitem__(self, item):
        """
        This function returns value assigned to given key.
        :param item: key belonging to this hash.
        :return: given key's value.
        """
        return self.connect.hget(self.get_instance_key(), item)

    def __delitem__(self, item):
        """
        Removes given key from hash.
        :param item: key to be removed.
        """
        self.changes[item].append(None)

    def __len__(self):
        """
        How many elements are in hash - as Redis says.
        :return: number of elements in hash.
        """
        return self.connect.hlen(self.get_instance_key())

    def keys(self):
        """
        This returns list of keys in hash.
        :return: list of keys in hash.
        """
        return self.connect.hkeys(self.get_instance_key())

    def items(self):
        """
        This returns key, value pairs available in this hash.
        :return: hash's key, value pairs.
        """
        return self.connect.hgetall(self.get_instance_key())

    def __contains__(self, item):
        """
        This functions checks for given key's existence in hash.
        :param item: key to be checked.
        :return: boolean
        """
        return self.connect.hexists(self.get_instance_key(), item)

    def __setitem__(self, item, value):
        """
        Assigns value to key in the hash.
        :param item: key
        :param value: value
        :return:
        """
        self.changes[item].append(value)

    def save(self):
        """
        This method analyzes changelist and using as few operations as possible propagates
        changes to Redis's Hash representing this instance.
        :return:
        """
        to_remove = []
        to_add = {}
        for key, value in self.changes.items():
            if value[-1] is None:
                to_remove.append(key)
            else:
                to_add[key] = value[-1]
        if to_remove:
            self.connect.hdel(self.get_instance_key(), *to_remove)
        if to_add:
            self.connect.hmset(self.get_instance_key(), to_add)
        self.changes.clear()

    def get_instance_key(self):
        """
        This function creates Redis's instance key.
        :return: key in which instance will be saved.
        """
        return self.get_key(self.name)

    @classmethod
    def get_key(cls, name):
        """
        This method creates a Redis key in which instance with given name will be saved.
        :param name: name of object for which a key is to be made.
        :return: key used in Redis for given name.
        """
        return name


class RedisList(object):
    """
    This class is a proxy for Redis List. It enables instant modifications to
    Redis entity. It has only basic operations pythonized at the moment.
    :type connect: redis.Redis
    """
    namespace = 'redis'

    def __init__(self, name, namespace=None):
        """
        This function initializes and remembers name of the hash.
        By default name is used as Redis key for this instance.

        :param namespace: name of connection used by this instance.
        :param name: name of hash.
        :return:
        """
        self.connect = RedisModelRegister(namespace or self.namespace).connect()
        self.name = name

    def clear(self):
        """
        This removes the list from Redis.
        :return:
        """
        self.connect.delete(self.get_instance_key())

    def __getitem__(self, item):
        """
        Returns value(s) for given index or slice [min, max].
        :param item: index or slice.
        :return: value or values.
        """
        if isinstance(item, slice):
            if item.start is None:
                start = 0
            else:
                start = item.start
            if item.stop is None:
                return self.connect.lrange(self.get_instance_key(), start, -1)
            return self.connect.lrange(self.get_instance_key(), start, item.stop)
        else:
            return self.connect.lrange(self.get_instance_key(), item, item)[0]

    def remove(self, item):
        """
        Removes all elements with given value.
        :param item: value to be removed.
        """
        self.connect.lrem(self.get_instance_key(), item, 0)

    def append(self, item):
        """
        Adds element at the end of the list.
        :param item: element to be appended.
        :return:
        """
        return self.connect.rpush(self.get_instance_key(), item)

    def prepend(self, item):
        """
        Adds element at the beginning of the list.
        :param item: element to be prepended.
        :return:
        """
        return self.connect.lpush(self.get_instance_key(), item)

    def pop(self, first=False):
        """
        Gets and removes an element from list's edge. By default it's the last element.
        :param first: Should the first element be popped instead of the last.
        :return:
        """
        if first:
            return self.connect.lpop(self.get_instance_key())
        else:
            return self.connect.rpop(self.get_instance_key())

    def __len__(self):
        """
        List length as returned by Redis.
        :return: number of elements in the list.
        """
        return self.connect.llen(self.get_instance_key())

    def __setitem__(self, item, value):
        """
        Assigns a value to given index.
        :param item: index
        :param value: value
        :return:
        """
        self.connect.lset(self.get_instance_key(), item, value)

    def get_instance_key(self):
        """
        This function creates Redis's instance key.
        :return: key in which instance will be saved.
        """
        return self.get_key(self.name)

    @classmethod
    def get_key(cls, name):
        """
        This method creates a Redis key in which instance with given name will be saved.
        :param name: name of object for which a key is to be made.
        :return: key used in Redis for given name.
        """
        return name
