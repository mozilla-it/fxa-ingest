from setuptools import setup, find_packages
import os

setup(name='fxa-ingest',
      version='0.0.1',
      description='Python libraries/scripts for various integrations',
      python_requires='>=3.5',
      author='Chris Valaas',
      author_email='cvalaas@mozilla.com',
      #packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      packages=['fxa_ingest'],
      scripts=[s for s in setuptools.findall('bin/') if os.path.splitext(s)[1] != '.pyc'],
      install_requires=[
        'google-cloud-pubsub==0.41.0',
        'google-cloud-spanner==1.9.0',
        'google-cloud-bigquery==1.17.0',
        'grpcio-gcp==0.2.2',   # this seems to be explicitly required on debian python 3.5  ?!
        'user-agents==2.0',
        'boto3==1.9.159',
        'click',
      ]
    )
