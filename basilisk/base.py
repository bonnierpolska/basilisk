"""
This module defines required design patterns and classes responsible for the Redis connection and
model register.
"""
import json
import uuid
from collections import defaultdict
from functools import wraps

import redis
from elasticsearch import Elasticsearch
from six import with_metaclass

from .fields import MapField


def singleton_decorator(function):
    """
    We embed given function into checking if the first (zeroth) parameter of its call
    shall be initialised. Additionally the parameter corresponding to NamedSingleton's
    group name is removed from the actual call.

    :param function: instantiating function (usually __init__).
    :returns: embedded function function.
    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        """
        This inner function checks init property of given instance and depending on its
        value calls the function or not.
        """
        if args[0].sl_init:
            new_args = list(args)
            if hasattr(args[0], 'sl_name'):
                del new_args[1]
            return function(*new_args, **kwargs)
    return wrapper


class SingletonCreator(type):
    """
    This metaclass wraps __init__ method of created class with singleton_decorator.
    It's to make sure that it's impossible to mess up the instance for example by
    calling __init__ with getattr.
    """
    def __new__(mcs, name, bases, attrs):
        """
        Wraps are awesome. Sometimes.
        """
        if not (len(bases) == 1 and object in bases):
            if '__init__' in attrs:
                attrs['__init__'] = singleton_decorator(attrs['__init__'])
        return super(SingletonCreator, mcs).__new__(mcs, name, bases, attrs)


class NamedSingletonBase(object):
    """
    Non-metaclassed base of NamedSingleton. Damn you, Python 2 and 3 differences.
    """
    _instances = defaultdict(lambda: None)

    def __new__(cls, *args):
        """
        This magic method override makes sure that only one instance with given
        group name (args[0]) will be created.
        """
        if len(args) < 1:
            raise TypeError("You need to pass group name as first argument to initialize this class.")
        group_name = args[0]
        if not isinstance(cls._instances[group_name], cls):
            cls._instances[group_name] = super(NamedSingletonBase, cls).__new__(cls)
            cls._instances[group_name].sl_init = True
            cls._instances[group_name].sl_name = group_name
        else:
            cls._instances[group_name].sl_init = False
        return cls._instances[group_name]


class NamedSingleton(with_metaclass(SingletonCreator, NamedSingletonBase)):
    """
    This class implements the named singleton pattern using a metaclass and
    overriding default __new__ magic method's behaviour. Only thing necessary
    to use it is having NamedSingleton as a base class (and SingletonMetaclass
    as a base for your metaclass, if you need one).

    Named singleton differs from regular singleton in that it enables you global
    access to a bunch of named instances. You can't create more than one instance
    with the same group name.

    You should pass the group name as the first parameter while getting
    a NamedSingleton instance.

    sl_init and sl_name properties are reserved, you can't use them in inheriting classes.
    """
    pass


class SingletonBase(object):
    """
    This class implements the singleton pattern using a metaclass and
    overriding default __new__ magic method's behaviour. Only thing necessary
    to use it is having Singleton as a base class (and SingletonMetaclass
    as a base for your metaclass, if you need one).

    sl_init property is reserved, you can't use it in inheriting classes.
    """

    _instance = None

    def __new__(cls, *args):
        """
        This magic method override makes sure that only one instance will be created.

        """
        if not isinstance(cls._instance, cls):
            cls._instance = super(SingletonBase, cls).__new__(cls)
            cls._instance.sl_init = True
        else:
            cls._instance.sl_init = False
        return cls._instance


class ConfigCreator(SingletonCreator):
    """
    This metaclass is just a hack to enable dict-like access and assure only one instance.
    """

    def __getitem__(cls, item):
        """
        Let's enable dict-like read-only access.

        :param item: item to get.
        :returns: item's value.
        """
        return getattr(cls, item)


class Config(with_metaclass(ConfigCreator, SingletonBase)):
    """
    This class keeps appropriate config data.

    It enables access as a class property and as an item (like in dict).
    """

    @classmethod
    def load(cls, **namespaces):
        """
        Initialize config from a list of params.
        """
        for namespace, value in namespaces.items():
            setattr(cls, namespace, value)

    def __getitem__(self, item):
        """
        Let's enable dict-like read-only access.

        :param item: item to get.
        :returns: item's value.
        """
        return getattr(self.__class__, item)


class MapModelRegister(object):
    """
    This class as model register for a NoSQL store.
    """

    def __init__(self):
        """
        We create an empty model dict.
        """
        self._models = {}
        self.connect = lambda: None

    def register(self, name, ref):
        """
        Register a model, shall we?

        :param name: model name.
        :param ref: model class.
        :returns: True if model was added just now, False if it was already in the register.

        """
        if not self.lookup(name):
            self._models[name] = ref
            return True
        return False

    def lookup(self, name):
        """
        I like to know if a model is in the register, don't you?

        :param name: name to check.
        :returns: True if model with given name is in the register, False otherwise.
        """
        return self._models.get(name, None)


class RedisModelRegister(NamedSingleton, MapModelRegister):
    """
    This class creates Redis connection pool and acts as model register.
    """

    def __init__(self):
        """
        We create a connection pool.
        """
        super(RedisModelRegister, self).__init__()
        self.pool = redis.ConnectionPool(**Config[self.sl_name])
        self.connect = lambda: redis.Redis(connection_pool=self.pool)


class ElasticsearchModelRegister(NamedSingleton, MapModelRegister):
    """
    This class creates ElasticSearch connection and acts as model register.
    """
    def __init__(self):
        """
        We create a connection pool.
        """
        super(ElasticsearchModelRegister, self).__init__()
        self.connect = lambda: Elasticsearch(**Config[self.sl_name])


class MapModelCreator(type):
    """
    This metaclass integrates classes with MapModelRegister, properly inherits
    MapFields from base classes and injects fields list and NoSQL store connection
    to created classes.
    """
    registers = {}
    register = lambda namespace: None

    @staticmethod
    def get_attrs_with_base(bases, attrs):
        """
        This function creates a list of all properties descending from MapField
        which were created directly in attrs or in base classes. Those properties
        are all removed from attrs.

        :param bases: base classes
        :param attrs: devlared properties of the class.
        :returns: 2-tuple containing a list of MapFields in class and its ancestors and a list of names of id fields.
        """
        args = {}
        id_fields = []
        for base in bases:
            # if we've encountered a model
            if '_fields' in base.__dict__ and base.get_fields():
                for key, item in base.get_fields().items():
                    if isinstance(item, MapField):
                        args[key] = item
                        if item.is_primary():
                            id_fields.append(key)
        for key, item in attrs.items():
            if isinstance(item, MapField):
                args[key] = item
                if item.is_primary():
                    id_fields.append(key)

        # Remove fields from attrs - they should only be in a private dict,
        # while the class and its instances should only keep a field's value
        # in field-named property. Additionally we're setting each field's
        # name as declared during class declaration.
        for key, item in args.items():
            item.set_name(key)
            if key in attrs:
                del attrs[key]
        return args, id_fields

    def __new__(mcs, name, bases, attrs):
        """
        This method creates and registers new class, if it's not already
        in the register, and injects it with a list of fields _fields,
        a NoSQL store connection function connect and primary key field's name.
        """
        # Do not modify the base classes, which actual models inherit.
        if bases[0].__bases__[0].__name__ != 'object':
            namespace = attrs.get('namespace', bases[0].namespace)
            if namespace not in mcs.registers:
                mcs.registers[namespace] = mcs.register(namespace)
            model = mcs.registers[namespace].lookup(name)
            if not model:
                attrs['_fields'], id_fields = mcs.get_attrs_with_base(bases, attrs)
                if not id_fields:
                    raise TypeError("No id/primary key field in {} class.".format(name))
                if len(id_fields) > 1:
                    raise TypeError("Multiple primary key in {} class.".format(name))
                attrs['id_field'] = id_fields[0]
                attrs['connect'] = mcs.registers[namespace].connect()
            model = super(MapModelCreator, mcs).__new__(mcs, name, bases, attrs)
            mcs.registers[namespace].register(name, model)
        else:
            model = super(MapModelCreator, mcs).__new__(mcs, name, bases, attrs)
        return model


class RedisModelCreator(MapModelCreator):
    """
    This class implements MapModelCreator for Redis.
    """
    register = RedisModelRegister


class ElasticsearchModelCreator(MapModelCreator):
    """
    This class implements MapModelCreator for Elasticsearch.
    """
    register = ElasticsearchModelRegister


class MapModelException(Exception):
    """
    Exception raised when errors related to NoSQL store handling are encountered.
    """
    pass


class MapModelBase(object):
    """
    This is the base class for NoSQL store models.

    This class enables reading object with given id, saving object and data
    (de)serialization.

    Redis connection is available in connect property.
    Dict of fields is available in _fields property.

    Reserved property names, apart from methods, are _fields, id_field and connect.

    :type _fields: dict
    :type id_field: str
    """
    MapModelException = MapModelException

    # It's to make sure syntax analyzers see the variables set by metaclass.
    _fields = None
    connect = None
    id_field = None

    def __init__(self, **kwargs):
        """
        We fill the instance using kwargs elements that are also fields or fields'
        default values.

        :param kwargs: initial values of fields.
        """
        for key, item in self._fields.items():
            self.__dict__[key] = kwargs[key] if key in kwargs else item.get_default()

    def get_instance_key(self):
        """
        This function returns a key in which the instance will live in NoSQL store.

        :returns: Redis key name containing instance hash
        """
        return self.get_key(getattr(self, self.id_field))

    def serialize(self, dump=False):
        """
        We try to call serialize for each field, if it doesn't exist then field's value
        is not converted.

        :param dump: whether the result should be json (True) or python dict (False).
        :returns: dictionary of values ready to be sent to NoSQL store.
        """
        ret = {k: (i.serialize(self.__dict__[k]) if hasattr(i, "serialize") else self.__dict__[k])
               for k, i in self._fields.items()}
        if dump:
            return json.dumps(ret)
        return ret

    def to_dict(self, *args):
        """
        This method returns a dict containing fields and their values in this instance.

        :returns: values dict.
        """
        ret = {k: self.__dict__[k] for k in self._fields if not args or k in args}
        return ret

    def _save(self, create_id):
        """
        This method performs store-agnostic part of saving.

        :param create_id: whether id should be created automatically if it's not set yet.
        """
        if not getattr(self, self.id_field):
            if create_id:
                setattr(self, self.id_field, str(uuid.uuid4()))
            else:
                raise ValueError("No primary key in class {} - fill the field \
                 {} or use create_id=True".format(self.__class__.__name__, self.id_field))

    def save(self, create_id=True):
        """
        Let's save instance's current state to NoSQL store.

        :param create_id: whether id should be created automatically if it's not set yet.
        :returns: self
        """
        raise NotImplementedError()

    @classmethod
    def get_fields(cls):
        """
        This function returns the dict of model fields.

        :returns: dict containing name: field pairs.
        """
        return cls._fields

    @classmethod
    def get_key(cls, oid):
        """
        This function creates a key in which NoSQL store will save the instance with given id.

        :param oid: id of object for which a key should be created.
        :returns: NoSQL store key.
        """
        raise NotImplementedError()

    @classmethod
    def pythonize(cls, data, loads=False):
        """
        This method prepares the data fetched from NoSQL store for new instance.

        :param data: values to convert.
        :returns: dict of values ready to pass to __init__.
        """
        if loads:
            data = json.loads(data)

        data = {
            key if isinstance(key, str) else key.decode('utf-8'): value
            for key, value in data.items()
        }
        return {key: cls.get_fields()[key].pythonize(value)
                for key, value in data.items()
                if key in cls.get_fields()}

    @classmethod
    def get(cls, oid):
        """
        This method gets a model instance with given id from NoSQL store.

        :param oid: id of object to get.
        :returns: hydrated model instance.
        """
        raise NotImplementedError()


class MapModel(with_metaclass(MapModelCreator, MapModelBase)):
    """
    This is a placeholder class to test basic metaclass and base class functionality.
    """
    pass
