# Databricks notebook source
# MAGIC %run ../sample/sample_notebook

# COMMAND ----------

import unittest
# from sample.sample_notebook import simple_repeat_word


class MyNotebookTests(unittest.TestCase):
    def test_simple_repeat_word(self):
        self.assertEqual(simple_repeat_word(3), 'wording wording wording ')
        self.assertNotEqual(simple_repeat_word(2), 'wording')


suite = unittest.TestLoader().loadTestsFromTestCase(MyNotebookTests)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------


