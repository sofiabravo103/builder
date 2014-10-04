#!/usr/bin/python2

import unittest, os, sys, ast
from os.path import dirname, abspath, basename
sys.path.append(dirname(dirname(abspath(__file__))))
from builder import main as builder

class TestBuilder(unittest.TestCase):
  @staticmethod
  def call_builder(parameters):
    builder(parameters)

  def test_arrivals(self):
    ''' it should generate the correct ammount of fixed arrivals'''
    test_name = 'test_arrivals'
    arrivals = 1
    tuples = 10000
    self.call_builder(['-s', str(tuples) ,'-d','3','--arrivals', arrivals, \
      '-o', test_name,'--events_per_line','1'])
    ids_dim_count = {}
    for id in range(0,tuples):
      ids_dim_count[id] = [0, 0, 0]

    test_file = open(test_name)
    for tupl_str in test_file:
      tupl = eval(tupl_str)
      id = tupl[1]
      dim = tupl[2]

      ids_dim_count[id][dim] += 1

    for id in range(0,tuples):
      for dim in range(0,3):
        self.assertTrue(ids_dim_count[id][dim] <= arrivals)

    os.system('rm ' + test_name)

  def test_poisson(self):
    '''it should generate the correct amount of poisson arrivals'''
    test_name = 'test_poisson'
    poissparameter = 1
    tuples = 10000
    self.call_builder(['-s', str(tuples) ,'-d','3','--poissparameter', \
      str(poissparameter), '-o', test_name, '--time', '650', \
      '--interval', '650', '--events_per_line', '1'])
    ids_dim_count = {}
    for id in range(0,tuples):
      ids_dim_count[id] = [0, 0, 0]

    test_file = open(test_name)
    for tupl_str in test_file:
      tupl = eval(tupl_str)
      id = tupl[1]
      dim = tupl[2]

      ids_dim_count[id][dim] += 1

    for dim in range(0,3):
      sum =  0
      for id in range(0,tuples):
        sum += ids_dim_count[id][dim]

      prom = int(round(sum / float(tuples),0))
      self.assertTrue(prom <= poissparameter)

    os.system('rm ' + test_name)

  def test_parse_ok(self):
    '''it should parse file completly with different events per line'''
    test_name = 'test_parse_ok'

    self.call_builder(['--autodataset','-o', test_name,'--events_per_line','1'])
    test_file = open(test_name)
    for tupl_str in test_file:
      tupl = eval(tupl_str)
      ts = tupl[0]
      id = tupl[1]
      dim = tupl[2]
      val = tupl[3]
      self.assertIsNotNone(ts)
      self.assertIsNotNone(id)
      self.assertIsNotNone(dim)
      self.assertIsNotNone(val)
    os.system('rm ' + test_name)

    self.call_builder(['--autodataset','-o', test_name, '--events_per_line','5000'])
    test_file = open(test_name)
    for tupl_str in test_file:
      tupl = eval(tupl_str)
      ts = tupl[0]
      id = tupl[1]
      dim = tupl[2]
      val = tupl[3]
      self.assertIsNotNone(ts)
      self.assertIsNotNone(id)
      self.assertIsNotNone(dim)
      self.assertIsNotNone(val)

    os.system('rm ' + test_name)


if __name__ == '__main__':
  unittest.main()
