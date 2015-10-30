"""
This module contains serializable fields ready to use in NoSQL store models.
"""
import json

from six import text_type


class MapField(object):
    """
    This is a base class for all NoSQL store fields. It supports data-based initialisation,
    default values and prepping values to serialization.
    """
    __slots__ = ('_type', '_default', '_name', '_key')

    def __init__(self, **kwargs):
        """
        This method sets basic properties for a field's instance.

        :param kwargs: may contain deserializing function 'type' (default unicode),
         default for default value (None), key determining whether a field is model's
         primary key and name (but that's better used by NoSQLModelCreator).
        """
        self._type = kwargs.get('type', lambda data: data if isinstance(data, str) else data.decode('utf-8'))
        self._default = kwargs.get('default', None)
        self._name = kwargs.get('name', None)
        self._key = kwargs.get('key', False)

    def get_default(self):
        """
        It returns field's default value. Surprise surprise.

        :returns: field's default value.
        """
        return self._default

    def is_primary(self):
        """
        Is the field model's primary key.

        :returns: boolean.
        """
        return self._key

    def set_name(self, name):
        """
        It sets field's name to make it self-conscious.

        :param name: field name as in model.
        :returns: self
        """
        self._name = name
        return self

    def get_name(self):
        """
        Returns field's name.

        :returns: this field's name.
        """
        return self._name

    @staticmethod
    def serialize(data):
        """
        Let's prepare the data to writing it in NoSQL store. By default it returns data
        without any changes.

        :param data: input data
        :returns: data ready to be written in NoSQL store.
        """
        return data

    def pythonize(self, data):
        """
        This function pythonizes data coming from NoSQL store.

        :param data: data fetched from NoSQL store.
        :returns: data in Python format.
        """
        return self._type(data) if self._type else data


class JsonMapField(MapField):
    """
    This class enables keeping JSON as field value.
    """

    def __init__(self, **kwargs):
        """
        Set the default value to empty dict.

        :param kwargs:
        """
        if 'default' not in kwargs:
            kwargs['default'] = {}
        super(JsonMapField, self).__init__(**kwargs)

    @staticmethod
    def serialize(data):
        """
        This function dumps data to JSON.

        :param data: input data.
        :returns: data dumped to JSON.
        """
        return json.dumps(data)

    def pythonize(self, data):
        """
        This function loads JSON data to Python objects.

        :param data: data fetched from NoSQL store.
        :returns: data in Python format.
        """
        if isinstance(data, text_type):
            return json.loads(data)
        return json.loads(text_type(data, 'utf-8'), 'utf-8')
