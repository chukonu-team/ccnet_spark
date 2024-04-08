#### setup.py
import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "ccnet_spark",
    version = "0.1.0",
    author = "SeeLey Wang",
    author_email = "zizdlp@gmail.com",
    description = ("a spark version for ccnet"),
    license = "BSD",
    keywords = "cc_net spark",
    url = "",
    packages=['ccnet_spark'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)
