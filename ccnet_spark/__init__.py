# ccnet_spark/__init__.py

__version__ = '0.1.0'
__author__ = 'SeeLey Wang'

from .load import open_read, parse_warc_file
from .text_normalizer import normalize_for_dedup
from .hash import NaiveHashSet,compute_hashes