"""
This module contains tests regarding correctness of nosql_map's Public API.
"""
import unittest

import redis
from six import string_types, b

from .base import RedisModelRegister, singleton_decorator, NamedSingleton, MapModel, Config, MapModelBase
from .fields import MapField, JsonMapField
from .redis_entities import RedisModel, RedisSortedSet, RedisHash, RedisModelException, RedisList
from .elasticsearch_entities import ElasticsearchModel, ElasticsearchModelException

Config.load(redis={'host': 'localhost', 'port': 6379, 'db': 0, 'max_connections': 10},
            elastic={})


class SingletonDecoratorTest(unittest.TestCase):
    """
    This test case checks the init-regulating decorator.
    """

    @classmethod
    def setUpClass(cls):
        """
        This method sets up properties required to run the tests.
        :return:
        """

        class Test(object):
            """
            This class enables properties, pure object doesn't.
            """

            def __init__(self, sl_init=True):
                """
                This method sets the parameter checked by singleton_decorator.
                """
                self.sl_init = sl_init

        cls.Test = Test

        def func(param):
            """
            This function is used to test the decorator.
            :param param:
            :return:
            """
            if param:
                return 1

        cls.decorated = staticmethod(singleton_decorator(func))
        cls.func = staticmethod(func)

    def test_properties(self):
        """
        This test checks if the function gets decorated properly.
        :return:
        """
        self.assertEqual(self.func.__name__, self.decorated.__name__)

    def test_init(self):
        """
        This test checks whether initialization is done when it should be.
        :return:
        """
        obj = self.Test(True)
        self.assertEqual(self.decorated(obj, 'A'), 1)
        obj = self.Test(False)
        self.assertEqual(self.decorated(obj, 'A'), None)

    def test_multiple_calls(self):
        """
        This test checks a more life-like decorator usage with multiple calls.
        :return:
        """
        obj = self.Test(True)
        self.assertEqual(self.decorated(obj, 'A'), 1)
        obj.sl_init = False
        self.assertEqual(self.decorated(obj, 'A'), None)
        self.assertEqual(self.decorated(obj, 'A'), None)
        self.assertEqual(self.decorated(obj, 'A'), None)
        obj.sl_init = True
        self.assertEqual(self.decorated(obj, 'A'), 1)
        self.assertEqual(self.decorated(obj, 'A'), 1)


class NamedSingletonTest(unittest.TestCase):
    """
    This test suite checks whether our extended singleton works as intended.
    """

    @classmethod
    def setUpClass(cls):
        """
        This method sets up a class required to proceed with the tests.
        :return:
        """

        class Test(NamedSingleton):
            """
            This class is the bare requirement to test NamedSingleton.
            """

            def __init__(self, param):
                """
                This function allows us to check how many times it was called.
                :param param:
                :return:
                """
                self.test = self.test + 1 if hasattr(self, 'test') else param
                self.var = None

            @classmethod
            def get_instances(cls):
                """
                This functions allows us an insight into class instances dict.
                :return: instances dict
                """
                return cls._instances

        cls.Test = Test

    def test_incorrect(self):
        """
        This checks whether improper call, without group name, will raise an exception.
        :return:
        """
        self.assertRaises(TypeError, self.Test)

    def tearDown(self):
        """
        This function clears class instances list after every test.
        :return:
        """
        self.Test.get_instances().clear()

    def test_same(self):
        """
        Let's test what will happen if we try to initialize for same group name multiple times.
        :return:
        """
        object_a = self.Test('A', 1)
        object_b = self.Test('A', 5)
        self.assertTrue(object_a is object_b)
        object_a.var = 1
        self.assertEqual(object_b.var, 1)
        self.assertEqual(object_b.test, 1)
        self.assertEqual(object_a.test, 1)

    def test_different(self):
        """
        Let's see what happens if we have inits for several group names.
        :return:
        """
        object_a = self.Test('A', 1)
        object_b = self.Test('B', 2)
        self.assertFalse(object_a is object_b)
        object_a.var = 1
        self.assertIsNone(object_b.var)
        object_b.var = 2
        self.assertNotEqual(object_a.var, object_b.var)
        self.assertEqual(object_a.var, 1)
        self.assertEqual(object_b.var, 2)
        self.assertEqual(object_a.test, 1)
        self.assertEqual(object_b.test, 2)


class ConfigTest(unittest.TestCase):
    """
    This case tests the config.
    """

    def test_init(self):
        """
        Let's check the attribute access.
        """
        self.assertEqual(Config['elastic'], {})
        self.assertEqual(Config()['redis'], Config['redis'])
        self.assertEqual(Config()['redis'], Config['redis'])


class ModelRegisterTest(unittest.TestCase):
    """
    This suite checks if model registry works correctly.
    """

    @classmethod
    def setUpClass(cls):
        """
        This method sets up Redis model register.
        :return:
        """
        cls.register = RedisModelRegister('redis')

    def test_connection(self):
        """
        This method tests if Redis connection is working properly. Duh.
        :return:
        """
        connection = self.register.connect()
        self.assertIsInstance(connection, redis.Redis)
        self.assertTrue(connection.set('test', 1))
        self.assertEqual(connection.delete('test'), 1)

    def test_register(self):
        """
        This method checks if we can register models correctly.
        :return:
        """
        self.assertIsNone(self.register.lookup('model'))
        model = object()
        self.assertTrue(self.register.register('model', model))
        self.assertEqual(self.register.lookup('model'), model)
        self.assertFalse(self.register.register('model', model))


class RedisFieldTest(unittest.TestCase):
    """
    This suite tests the correctness of flying rainbow unicorns. Really. It's not about
    Redis Fields at all.
    """

    def test_all(self):
        """
        Let's see if RedisField and JsonRedisField's public API works regardless
        of those unicorns.
        :return:
        """
        field_a = MapField(name='field_a', default='test', key=True)
        field_b = MapField(name='field_b', type=int)
        self.assertEqual(field_a.get_default(), 'test')
        self.assertIsNone(field_b.get_default())
        self.assertEqual(field_a.is_primary(), True)
        self.assertEqual(field_b.is_primary(), False)
        self.assertIsInstance(field_a.pythonize('field_a'), string_types)
        self.assertIsInstance(field_b.pythonize('1'), int)
        self.assertEqual(field_a.serialize('field_a'), 'field_a')
        field_a.set_name('c')
        self.assertEqual(field_a.get_name(), 'c')
        json_field = JsonMapField(name='field_c')
        self.assertEqual(json_field.pythonize(json_field.serialize({'a': [1, 2]}))['a'][1], 2)
        self.assertEqual(json_field.pythonize(u'{"a": 2}'), {'a': 2})
        self.assertEqual(json_field.pythonize(b('{"a": 2}')), {'a': 2})


class RedisModelTest(unittest.TestCase):
    """
    This test suite checks if RedisModel is working as intended.
    """

    @classmethod
    def setUpClass(cls):
        """
        We need to create a couple of models to proceed with the tests.
        """

        class Model(RedisModel):
            """
            Inner model to test reading and writing correctness.
            """
            name = MapField(key=True)
            value = MapField()

        class Inheriting(Model):
            """
            Inner model to check inheritance.
            """
            fame = MapField(type=int)

        cls.Model = Model
        cls.Inheriting = Inheriting

    def test_bad_model(self):
        """
        This method tests if errors are raised correctly when a class is improperly
        declared.
        :return:
        """
        self.assertRaises(TypeError, lambda: RedisModel.__metaclass__(
            'BadModel',
            (RedisModel,),
            {'name': MapField()}
        ))
        self.assertRaises(TypeError, lambda: RedisModel.__metaclass__(
            'BadModel',
            (RedisModel,),
            {'name': MapField(key=True), 'fame': MapField(key=True)}
        ))

        bad_model = self.Model()
        self.assertRaises(NotImplementedError, lambda: MapModelBase.save(bad_model))
        self.assertRaises(NotImplementedError, lambda: MapModel.get(1))
        self.assertRaises(NotImplementedError, lambda: MapModel.get_key(1))

    def test_inheritance(self):
        """
        This function checks fields inheritance.
        :return:
        """
        self.assertTrue('name' in self.Inheriting.get_fields())
        self.assertEqual(self.Inheriting.id_field, 'name')

    def test_save_and_select(self):
        """
        Move along. Nothing more than what's said in method name happens here.
        :return:
        """
        inheriting = self.Inheriting(name='test', fame=2, value='over 9000')
        inheriting.save()
        loaded = self.Inheriting.get(inheriting.name)
        self.assertEqual(loaded.name, inheriting.name)
        self.assertEqual(loaded.fame, inheriting.fame)
        self.assertEqual(loaded.value, inheriting.value)
        self.assertRaises(RedisModelException, lambda: self.Inheriting.get(123456))

    def test_dump(self):
        """
        We shall make sure all kinds of dumping (Python or JSON) are working.
        :return:
        """
        dictionary = dict(name='test', fame=2, value='over 9000')
        inheriting = self.Inheriting(**dictionary)
        loaded = inheriting.pythonize(inheriting.serialize(dump=True), loads=True)
        for key, value in dictionary.items():
            self.assertEqual(value, loaded[key])
        loaded = inheriting.to_dict()
        for key, value in dictionary.items():
            self.assertEqual(value, loaded[key])

    def test_create_id(self):
        """
        YOU SHALL NOT PASS if a model instance's id is not autogenerated properly.
        :return:
        """
        inheriting = self.Inheriting(fame=2, value='over 9000')
        self.assertRaises(ValueError, lambda: inheriting.save(create_id=False))
        inheriting.save()
        loaded = self.Inheriting.get(inheriting.name)
        self.assertEqual(loaded.name, inheriting.name)
        self.assertEqual(loaded.fame, inheriting.fame)
        self.assertEqual(loaded.value, inheriting.value)


class RedisSortedSetTest(unittest.TestCase):
    """
    This suite checks if RedisSortedSet works correctly.
    """

    def test_save_and_load(self):
        """
        This test checks RedisSortedSet's public API.
        :return:
        """
        redis_ss = RedisSortedSet('rss_test')
        redis_ss.clear()
        redis_ss.set_score('a', 1)
        redis_ss.set_score('b', 2)
        redis_ss.set_score('c', 3)
        redis_ss.set_score('d', 4)
        redis_ss.set_score('e', 5)
        redis_ss.save()
        redis_ss.set_score('a', 0)
        redis_ss.delete_item('e')
        redis_ss.save()
        self.assertEqual(redis_ss.lowest(), (b('a'), 0.0))
        self.assertEqual(redis_ss.highest(), (b('d'), 4.0))
        self.assertEqual(redis_ss[0][0], b('a'))
        self.assertEqual(redis_ss[0:][:], [b(x) for x in ['a', 'b', 'c', 'd']])
        self.assertEqual(redis_ss[0:][0:2], [b(x) for x in ['a', 'b']])
        self.assertEqual(len(redis_ss), 4)
        del redis_ss[0]
        self.assertEqual(len(redis_ss), 3)
        self.assertEqual(redis_ss[0:][:], [b(x) for x in ['b', 'c', 'd']])
        del redis_ss[:]
        self.assertEqual(len(redis_ss), 0)


class RedisHashTest(unittest.TestCase):
    """
    This suite checks if RedisHash works correctly.
    """

    def test_save_and_load(self):
        """
        This test checks RedisHash's public API.
        :return:
        """
        redis_hash = RedisHash('rh_test')
        redis_hash.clear()
        redis_hash['a'] = 1
        redis_hash['b'] = 2
        redis_hash['c'] = 3
        redis_hash['d'] = 4
        redis_hash['e'] = 5
        redis_hash['e'] = 6
        redis_hash.save()
        self.assertEqual(redis_hash.get('a', 'b', 'c'), [b(x) for x in ['1', '2', '3']])
        self.assertEqual(int(redis_hash['e']), 6)
        self.assertEqual(int(redis_hash['a']), 1)
        del redis_hash['a']
        redis_hash['b'] = 3
        redis_hash.save()
        self.assertIsNone(redis_hash['a'])
        self.assertEqual(int(redis_hash['b']), 3)
        self.assertEqual(len(redis_hash), 4)
        self.assertEqual(set(redis_hash.keys()), {b(x) for x in {'b', 'c', 'd', 'e'}})
        self.assertEqual(redis_hash.items(), {b(k): b(v) for k, v in {'b': '3', 'c': '3', 'd': '4', 'e': '6'}.items()})
        self.assertIn('b', redis_hash)


class RedisListTest(unittest.TestCase):
    """
    This suite checks if RedisList works correctly.
    """

    def test_save_and_load(self):
        """
        This test checks RedisList's public API.
        :return:
        """
        redis_list = RedisList('rl_test')
        redis_list.clear()
        redis_list.append(1)
        redis_list.append(2)
        redis_list.append(3)
        redis_list.append(4)
        redis_list.append(5)
        redis_list.prepend(0)
        self.assertEqual(len(redis_list), 6)
        self.assertEqual(int(redis_list.pop()), 5)
        self.assertEqual(int(redis_list.pop(True)), 0)
        self.assertEqual(len(redis_list), 4)
        self.assertEqual(int(redis_list[1]), 2)
        self.assertEqual([int(item) for item in redis_list[:]], [1, 2, 3, 4])
        self.assertEqual([int(item) for item in redis_list[1:]], [2, 3, 4])
        self.assertEqual([int(item) for item in redis_list[1:2]], [2, 3])
        redis_list.remove(1)
        self.assertEqual(len(redis_list), 3)
        self.assertEqual(int(redis_list[0]), 2)
        redis_list[0] = 13
        self.assertEqual(int(redis_list[0]), 13)


class ElasticsearchModelTest(unittest.TestCase):
    """
    This test suite checks if ElasticsearchModel is working as intended.
    """

    @classmethod
    def setUpClass(cls):
        """
        We need to create a couple of models to proceed with the tests.
        """

        class Model(ElasticsearchModel):
            """
            Inner model to test reading and writing correctness.
            """
            name = MapField(key=True)
            value = MapField()

        class Inheriting(Model):
            """
            Inner model to check inheritance.
            """
            fame = MapField(type=int)

        cls.Model = Model
        cls.Inheriting = Inheriting

    def test_inheritance(self):
        """
        This function checks fields inheritance.
        :return:
        """
        self.assertTrue('name' in self.Inheriting.get_fields())
        self.assertEqual(self.Inheriting.id_field, 'name')

    def test_save_and_select(self):
        """
        Move along. Nothing more than what's said in method name happens here.
        :return:
        """
        inheriting = self.Inheriting(name='test', fame=2, value='over 9000')
        inheriting.save()
        loaded = self.Inheriting.get(inheriting.name)
        self.assertEqual(loaded.name, inheriting.name)
        self.assertEqual(loaded.fame, inheriting.fame)
        self.assertEqual(loaded.value, inheriting.value)
        self.assertRaises(ElasticsearchModelException, lambda: self.Inheriting.get(123456))

    def test_dump(self):
        """
        We shall make sure all kinds of dumping (Python or JSON) are working.
        :return:
        """
        dictionary = dict(name='test', fame=2, value='over 9000')
        inheriting = self.Inheriting(**dictionary)
        loaded = inheriting.pythonize(inheriting.serialize(dump=True), loads=True)
        for key, value in dictionary.items():
            self.assertEqual(value, loaded[key])
        loaded = inheriting.to_dict()
        for key, value in dictionary.items():
            self.assertEqual(value, loaded[key])

    def test_create_id(self):
        """
        YOU SHALL NOT PASS if a model instance's id is not autogenerated properly.
        :return:
        """
        inheriting = self.Inheriting(fame=2, value='over 9000')
        self.assertRaises(ValueError, lambda: inheriting.save(create_id=False))
        inheriting.save()
        loaded = self.Inheriting.get(inheriting.name)
        self.assertEqual(loaded.name, inheriting.name)
        self.assertEqual(loaded.fame, inheriting.fame)
        self.assertEqual(loaded.value, inheriting.value)

if __name__ == '__main__':
    unittest.main()
