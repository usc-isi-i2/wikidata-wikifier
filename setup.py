from setuptools import setup
from setuptools.command.install import install

setup(name='wikidata-wikifier',
      version='0.1',
      description='a module used to find and transform the input to the corresponding Q nodes in wikidata',
      long_description=open('README.md').read(),
      author='USC ISI',
      url='https://github.com/usc-isi-i2/wikidata-wikifier.git',
      maintainer_email='amandeep@isi.edu',
      maintainer='Amandeep Singh',
      license='MIT',
      packages=['wikifier',],
      zip_safe=False,
      python_requires='>=3.6',
      install_requires=[

      ],
      keywords='wikidata-wikifier',
      )
