from setuptools import setup
from setuptools.command.install import install

setup(name='wikidata-wikifier',
      version='1.0.0',
      description='a module used to find and transform the input to the corresponding Q nodes in wikidata',
      long_description=open('README.md').read(),
      author='USC ISI',
      url='https://github.com/usc-isi-i2/wikidata-wikifier.git',
      maintainer_email='junliu@isi.edu',
      maintainer='Jun Liu',
      license='MIT',
      packages=['wikifier',],
      zip_safe=False,
      python_requires='>=3.6',
      install_requires=[
          'SPARQLWrapper>=1.8.4', 'requests', 'datasketch', 'pandas'
      ],
      keywords='wikidata-wikifier',
      )
