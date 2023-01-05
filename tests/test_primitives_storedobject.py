from os import getcwd
from pathlib import Path
from shutil import rmtree
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from yaml import safe_load, safe_dump

from primitives import StoredObject


class BaseStoredObjectTests(unittest.TestCase):
    @patch('primitives.StoredObject.__abstractmethods__', set())
    def setUp(self):
        self.dir = Path(getcwd(), '_data')
        self.object = StoredObject(root=self.dir)

        # clear directory
        if self.dir.is_dir():
            rmtree(self.dir)
        self.dir.mkdir(parents=True)

    def tearDown(self):
        rmtree(self.dir)


class StoredObjectSerialization(BaseStoredObjectTests):
    def test_save(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_df'
        setattr(self.object, _attr_name, pd.DataFrame())
        # TODO: shouldn't an error be raised when adding a frame that didn't previously exist

        fn = ('literals.yml', f"{_attr_name}.yml",)
        with patch('primitives.StoredObject._instance_dir',
                   new_callable=PropertyMock(return_value=Path(self.dir, 'test_object'))):
            self.object.save()
            _files = [i.name for i in self.object._instance_dir.iterdir()]
        for i in _files:
            self.assertIn(i, fn)
        for i in fn:
            self.assertIn(i, _files)

        # TODO: verify file contents

    def test_load_invalid(self):
        """ Test when an attribute that isn't part of instance attributes tries to get added via
        literal storage.
        """
        with patch('primitives.StoredObject._instance_dir',
                   new_callable=PropertyMock(return_value=Path(self.dir, 'test_object'))):
            self.object.save()
            _fn = Path(self.object._instance_dir, 'literals.yml')
            with open(_fn, 'r') as f:
                literals: dict = safe_load(f)

            self.assertIsInstance(literals, dict)
            literals['test'] = 'test'
            with open(_fn, 'w') as f:
                safe_dump(literals, f)

            with self.assertRaises(AssertionError):
                self.object.load()

    def test_load(self):
        # test an arbitrary sequenced attribute
        _attr_name = 'mock_df'
        setattr(self.object, _attr_name, pd.Series())

        with patch('primitives.StoredObject._instance_dir',
                   new_callable=PropertyMock(return_value=Path(self.dir, 'test_object'))):
            self.object.save()
            _arbitrary_series_data = {1: 'test'}
            with open(Path(self.object._instance_dir, f"{_attr_name}.yml"), 'w') as f:
                safe_dump(_arbitrary_series_data, f)

            _fn = Path(self.object._instance_dir, 'literals.yml')
            with open(_fn, 'r') as f:
                literals: dict = safe_load(f)
            self.assertIsInstance(literals, dict)
            literals['root'] = 'test'
            with open(_fn, 'w') as f:
                safe_dump(literals, f)

            self.object.load()
        self.assertTrue(hasattr(self.object, 'root'))
        self.assertEqual(getattr(self.object, 'root'), 'test')

        # test arbitrary data
        self.assertEqual(_arbitrary_series_data, getattr(self.object, _attr_name).to_dict())

        # TODO: test that dataframes are properly loaded

    def test_save_exclude(self):
        # test assert that excluded attribute is not serialized
        _attr_name = 'mock_df'
        _excluded = 'excluded'
        setattr(self.object, _attr_name, pd.DataFrame())
        self.object.exclude = [_excluded]

        excluded = f"{_excluded}.yml"
        with patch('primitives.StoredObject._instance_dir',
                   new_callable=PropertyMock(return_value=Path(self.dir, 'test_object'))):
            self.object.save()
            _files = [i.name for i in self.object._instance_dir.iterdir()]
        self.assertNotIn(excluded, _files)


if __name__ == '__main__':
    unittest.main()
