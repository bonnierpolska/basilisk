"""
This module defines a Elasticsearch-backed model.
"""

from elasticsearch.exceptions import NotFoundError
from six import with_metaclass

from .base import ElasticsearchModelCreator, MapModelBase, MapModelException


__all__ = ['ElasticsearchModel', 'ElasticsearchModelException']


class ElasticsearchModelException(MapModelException):
    """
    Exception raised when errors related to Redis handling are encountered.
    """
    pass


class ElasticsearchModel(with_metaclass(ElasticsearchModelCreator, MapModelBase)):
    """
    This is the base class for Elasticsearch models. Internally they are just a Elasticsearch entity.
    This class enables reading object with given id, saving object and data
    (de)serialization.

    Elasticsearch connection is available in connect property.
    Dict of fields is available in _fields property.

    Reserved property names, apart from methods, are _fields, id_field and connect.

    :type connect: elasticsearch.Elasticsearch
    """

    MapModelException = ElasticsearchModelException
    ElasticsearchModelException = ElasticsearchModelException

    namespace = 'elastic'
    connect = None

    def save(self, create_id=True):
        """
        Let's save instance's current state to Elasticsearch.

        :param create_id: whether id should be created automatically if it's not set yet.
        """
        self._save(create_id)
        params = self.get_instance_key()
        params['body'] = self.serialize()
        self.connect.index(**params)
        return self

    @classmethod
    def get_key(cls, oid=None):
        """
        This function creates a key in which Elasticsearch will save the instance with given id.

        :param oid: id of object for which a key should be created.
        :returns: Elasticsearch key (index, document and id).
        """
        key = {'index': cls.__module__.replace('__', ''), 'doc_type': cls.__name__}
        if oid:
            key['id'] = oid
        return key

    @classmethod
    def get(cls, oid):
        """
        This method gets a model instance with given id from Elasticsearch.

        :param oid: id of object to get.
        :returns: hydrated model instance.
        """
        try:
            data = cls.connect.get(**cls.get_key(oid))['_source']
        except NotFoundError:
            data = None
        if data:
            return cls(**cls.pythonize(data))
        raise ElasticsearchModelException('No object with primary key {} of class {}'.format(cls.get_key(oid),
                                                                                             cls.__name__))
