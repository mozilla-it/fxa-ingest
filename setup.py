from setuptools import setup, find_packages
import os

setup(name='fxa-ingest',
      version='0.0.1',
      description='Python libraries/scripts for various integrations',
      python_requires='>=3.6',
      author='Chris Valaas',
      author_email='cvalaas@mozilla.com',
      packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
      scripts=[s for s in setuptools.findall('bin/') if os.path.splitext(s)[1] != '.pyc'],
      install_requires=[
        'google-cloud-pubsub==0.41.0',
        'google-cloud-spanner==1.9.0',
        'user-agents==2.0',
        'boto==2.49.0',
      ]
    )
