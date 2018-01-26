#!/usr/bin/env python
# encoding=utf8 ---------------------------------------------------------------
# Project           : NAME
# -----------------------------------------------------------------------------
# Author            : FFunction
# License           : BSD License
# -----------------------------------------------------------------------------
# Creation date     : YYYY-MM-DD
# Last modification : YYYY-MM-DD
# -----------------------------------------------------------------------------

import sys
from .catalogue import Catalogue
from .dedup import Dedup

try:
	import reporter
	logging = reporter.bind(sys.argv[0].split("/")[-1])
except ImportError:
	import logging

def cat(args=None):
	"""Writes out the catalogue at the given location."""
	args = sys.argv[1:] if args is None else args
	paths = args
	cat = Catalogue(paths, logging=logging)
	cat.write(sys.stdout)

def dedup(args=None):
	args = sys.argv[1:] if args is None else args
	cat = Dedup(args[0], logging=logging)


# EOF - vim: ts=4 sw=4 noet
