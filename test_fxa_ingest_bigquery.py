#!/usr/bin/env python3

import unittest
import os

os.environ['PUBSUB_PROJECT_ID'] = 'FAKE'
os.environ['FXA_PUBSUB_SUBSCRIPTION_FOR_BIGQUERY'] = 'FAKE'
os.environ['BIGQUERY_PROJECT_ID'] = 'FAKE'

from fxa_ingest import fxa_ingest_bigquery

class TestTransforms(unittest.TestCase):

  def setUp(self):
    self.fields_and_lengths = {
      'service'         : 100,
      'country'         : 200,
      'country_code'    : 200,
      'newsletters'     : None,
      'useragent'       : 1000,
      'os'              : 100,
      'os_ver'          : 100,
      'browser'         : 255,
      'metrics_context' : None,
      'ts'              : None,
      'device_type'     : None,
      'uid'             : 500,
      'device_id'       : 500,
      'device_count'    : None,
      'email'           : 255,
      'marketing_opt_in': None,
      'locale'          : 200,
      'lang'            : None,
    }
    self.tf = fxa_ingest_bigquery.transform

  def test_lengths(self):
    test_string = 'a' * 10000
    for field_name in self.fields_and_lengths:
      if self.fields_and_lengths[field_name]:

        input_dict = {}
        if field_name == 'country_code':
          input_dict['countryCode'] = test_string
        elif field_name == 'metrics_context':
          input_dict['metricsContext'] = { 'utm_whatever': test_string }
        elif field_name == 'device_id':
          input_dict['id'] = test_string
        elif field_name == 'device_count':
          input_dict['deviceCount'] = test_string
        elif field_name == 'marketing_opt_in':
          input_dict['marketingOptIn'] = test_string
#        elif field_name == 'useragent' or field_name == 'os' or field_name == 'os_ver' or field_name == 'browser':
#          # hmm, changing type? TODO: fix the module to not accept different types for this arg
#          input_dict = test_string
        else:
          input_dict[field_name] = test_string

        self.assertEqual( len(self.tf(field_name, input_dict)),
                          self.fields_and_lengths[field_name],
                          msg="%s length test" % field_name)
        self.assertEqual( self.tf(field_name, {}),
                          None,
                          msg="%s None test" % field_name)

  def test_service(self):
    self.assertEqual( self.tf('service', {}), None)
    self.assertEqual( self.tf('service', {'service': 'fake_service'}), 'fake_service')
    self.assertEqual( self.tf('service', {'service': 'a' * 200}), 'a' * 100)

  def test_newsletters(self):
    self.assertEqual( self.tf('newsletters', {}), [])
    self.assertEqual( self.tf('newsletters',
                      {'newsletters': ['fake_country']}), ['fake_country'])
    self.assertEqual( self.tf('newsletters', {'newsletters': 'a' * 200}), 'a' * 200)

  def test_metrics_context(self):
    self.assertEqual( self.tf('metrics_context', {}), 'https://accounts.firefox.com/')
    self.assertEqual( self.tf('metrics_context',
                      {'metricsContext': {'utm_something': 'a_value'}}),
                      'https://accounts.firefox.com/?utm_something=a_value')

  def test_ts(self):
    self.assertEqual( self.tf('ts', {}), None)
    self.assertEqual( self.tf('ts', {'ts': '1566255704'}),
                      '2019-08-19T23:01:44Z')

  def test_device_type(self):
    self.assertEqual( self.tf('device_type', {}), 'is_placeholder')
    self.assertEqual( self.tf('device_type', {'type': 'pickle'}),
                      'pickle')

  def test_device_count(self):
    self.assertEqual( self.tf('device_count', {}), 0)
    self.assertEqual( self.tf('device_count', {'deviceCount': 8}),
                      8)

  def test_marketing_opt_in(self):
    self.assertEqual( self.tf('marketing_opt_in', {}), False)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':  'yes'}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':  'Yes'}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':    'y'}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':      1}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn': 'True'}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':    't'}), True)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':    'f'}), False)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':      0}), False)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':   'no'}), False)
    self.assertEqual( self.tf('marketing_opt_in', {'marketingOptIn':      5}), False)

  def test_lang(self):
    self.assertEqual( self.tf('lang', {}), '')
    self.assertEqual( self.tf('lang', {'locale':  'en-US'}), 'en-US')
    self.assertEqual( self.tf('lang', {'locale':  'en-US;q=0.5'}), 'en-US')
    self.assertEqual( self.tf('lang', {'locale':  'fr-FR;q=1,en-US'}), 'fr-FR')

if __name__ == '__main__':
  #unittest.main(verbosity=5)
  unittest.main()




