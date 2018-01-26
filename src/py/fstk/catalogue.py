#!/usr/bin/env python3
# encoding=utf8 ---------------------------------------------------------------
# Project           : NAME
# -----------------------------------------------------------------------------
# Author            : FFunction
# License           : BSD License
# -----------------------------------------------------------------------------
# Creation date     : 2015-07-27
# Last modification : 2018-01-26
# -----------------------------------------------------------------------------

import os, stat, fnmatch, logging

TYPE_BASE    = "B"
TYPE_ROOT    = "R"
TYPE_DIR     = "D"
TYPE_FILE    = "F"
TYPE_SYMLINK = "S"
TYPES        = (TYPE_BASE, TYPE_ROOT, TYPE_DIR, TYPE_FILE, TYPE_SYMLINK)

def utf8(s):
	"""Ensures that the given string is in UTF-8"""
	return s.encode("utf8", "replace").decode("utf8")

# -----------------------------------------------------------------------------
#
# FILTER
#
# -----------------------------------------------------------------------------

class Filter(object):
	"""A filter class that allows to include/exclude (path, type) couples."""

	def __init__( self, types=None, names=None ):
		self._includeName = names or []
		self._includeType = [_.upper() for _ in (_[0] for _ in types)] if types else []
		self._excludeName = [_ for _ in (_[0] for _ in names)] if names else []
		self._excludeType = []

	def match( self, path, type=None ):
		"""Tells if the given path/type couple matched the filter. If `type`
		is `None`, it will be retrieved from the filesystem."""
		name = os.path.basename(path)
		# Dynamically retrieves the type, if necessary
		if type is None:
			if os.path.islink(path):
				type = TYPE_SYMLINK
			elif os.path.isdir(path):
				type = TYPE_DIR
			else:
				type = TYPE_FILE
		# We process exclusions first
		# for p in self._excludeName:
		# 	if fnmatch.fnmatch(name, p):
		# 		return False
		# Then we match the type
		matched = type in self._includeType if self._includeType else True
		# Then we proceeed with the name
		if matched:
			for p in self._includeName:
				if fnmatch.fnmatch(name, p):
					matched = True
		return matched

# -----------------------------------------------------------------------------
#
# CATALOGUE
#
# -----------------------------------------------------------------------------

class Catalogue(object):
	"""The catalogue object maintains a text-based list of all the files
	walked in a directory. The catalogue serves as a base for quickly
	iterating through a filesystem tree."""

	# SEE: https://en.wikipedia.org/wiki/Delimiter
	FIELD_SEPARATOR = chr(31)
	LINE_SEPARATOR  = "\n"

	def __init__( self, paths=(), base=None, filter=None, logging=logging ):
		"""Creates a new catalogue with the given `base` path, given
		list of `paths` and optional `filter`."""
		base        = base or os.path.commonprefix(paths)
		if not os.path.exists(base) or not os.path.isdir(base): base = os.path.dirname(base)
		self.base   = base
		self.paths  = [_ for _ in paths]
		for _ in self.paths:
			assert _.startswith(base)
		self.filter = filter
		self.logging = logging

	def walk( self ):
		"""Walks all the catalogue's `paths` and yields triples `(index, type, path)`."""
		counter = 0
		yield (counter, TYPE_BASE, self.base)
		for p in self.paths:
			mode = os.lstat(p)[stat.ST_MODE]
			if stat.S_ISCHR(mode):
				self.logging.info("Catalogue: Skipping special device file: {0}".format(utf8(p)))
			elif stat.S_ISBLK(mode):
				self.logging.info("Catalogue: Skipping block device file: {0}".format(utf8(p)))
			elif stat.S_ISFIFO(mode):
				self.logging.info("Catalogue: Skipping FIFO file: {0}".format(utf8(p)))
			elif stat.S_ISSOCK(mode):
				self.logging.info("Catalogue: Skipping socket file: {0}".format(utf8(p)))
			elif os.path.isfile(p) and self.match(p, TYPE_FILE):
				yield (counter, TYPE_ROOT, os.path.dirname(p))
				counter += 1
				yield (counter, TYPE_FILE, os.path.basename(p))
			elif os.path.islink(p) and self.match(p, TYPE_SYMLINK):
				yield (counter, TYPE_ROOT, os.path.dirname(p))
				counter += 1
				yield (counter, TYPE_SYMLINK, os.path.basename(p))
			elif self.match(p, TYPE_DIR):
				for root, dirs, files in os.walk(p, topdown=True):
					self.logging.info("Catalogue:\t#{3:010d}\t{0:04d}f+{1:04d}d\t{2}".format(len(files), len(dirs), utf8(root), counter))
					yield (counter, TYPE_ROOT, root)
					for name in files:
						path = os.path.join(root, name)
						type = TYPE_SYMLINK if os.path.islink(path) else TYPE_FILE
						if self.match(path, type):
							yield (counter, type, name)
							counter += 1
					for name in dirs:
						path = os.path.join(root, name)
						if self.match(path, TYPE_DIR):
							yield (counter, TYPE_DIR, name)
							counter += 1
			else:
				self.logging.info("Catalogue: Filtered out path: {0}".format(utf8(p)))

	def match( self, path, type ):
		"""Tells if the given path/type matches the filter, if any is available."""
		return self.filter.match(path, type) if self.filter else True

	def write( self, output ):
		"""Writes the catalogue to the given output, this triggers a walk
		of the catalogue."""
		for i, t, p in self.walk():
			assert t in TYPES
			line = ("{0}{3}{1}{3}{2}{4}".format(i,t,p, self.FIELD_SEPARATOR, self.LINE_SEPARATOR))
			output.write(line)

	def save( self, path ):
		"""Saves the catalogue to the given `path`. This will in turn call
		`write()`."""
		d = os.path.dirname(path)
		if not os.path.exists(d):
			self.logging.info("Catalogue: creating catalogue directory {0}".format(utf8(d)))
			os.makedirs(d)
		with open(path, "wb") as f:
			self.write(f)

# -----------------------------------------------------------------------------
#
# CATALOGUE READER
#
# -----------------------------------------------------------------------------

class CatalogueReader(object):
	"""Reads a catalogue file and provides hooks."""

	def __init__( self, positionPath=None ):
		self._positionPath = positionPath
		self.base = None
		self.root = None

	def read( self, path, range=None, resume=False ):
		"""Reads the catalogue at the given path and iterates throught the results.""
		listed in the catalogue. Note that this expects the catalogue to
		be in traversal order."""
		# We make sure everything is properly initialized
		if self.logging: self.logging.info("Opening catalogue: {0}".format(path))
		# The base is the common prefix/ancestor of all the paths in the
		# catalogue. The root changes but will always start with the base.
		base      = None
		root      = None
		self.base = None
		self.root = None
		# When no range is specified, we look for the index path
		# and load it.
		if range is None and resume:
			# TODO: and os.stat(path)[stat.ST_MTIME] <= os.stat(self._positionPath)[stat.ST_MTIME]:
			i = self.getLastPosition()
			range = (i, -1) if i else None
		# Now we iterate on the lines
		with open(path, "r") as f:
			for line in f:
				j_t_p     = line.split(Catalogue.FIELD_SEPARATOR, 2)
				if len(j_t_p) != 3:
					self.logging.error("Malformed line, expecting at least 3 colon-separated values: {0}".format(repr(line)))
					continue
				j, t, p   =  j_t_p
				p = p[:-1]
				i         = int(j) ; self.last = i
				if t == TYPE_BASE:
					# The first line of the catalogue is expected to be the base
					# it is also expected to be absolute.
					self.base = base = p
					self.onBase(base, i)
				elif t == TYPE_ROOT:
					# If we found a root, we ensure that it is prefixed with the
					# base
					assert base, "Catalogue must have a base directory before having roots"
					assert os.path.normpath(p).startswith(os.path.normpath(base)), "Catalogue roots must be prefixed by the base, base={0}, root={1}".format(utf8(base), utf8(p))
					# Now we extract the suffix, which is the root minus the base
					# and no leading /
					self.root = root = p
					self.onRoot(root, i)
				else:
					# We skip the indexes that are not within the range, if given
					if range:
						if i < range[0]:
							continue
						if len(range) > 1 and range[1] >= 0 and i > range[1]:
							self.logging.info("Reached end of range {0} >= {1}".format(i, range[1]))
							break
					# We check if the filter matches
					if self.matchFile(p, t, i):
						self.onFile(os.path.join(root, p), t, i)
				# We sync the database every 1000 item
				if j.endswith("000") and (not range or i>=range[0]):
					self.logging.info("{0} items processed, syncing db".format(i))
					self._savePosition(j)
					self.onSync(j)


	# =========================================================================
	# OVERRIDES
	# =========================================================================

	def onBase( self, path, index):
		pass

	def onRoot( self, path, index):
		pass

	def onFile( self, path, type, index):
		pass

	def onSync( self, index ):
		pass

	def matchFile( self, path, type, index ):
		return True

	# =========================================================================
	# POSITION
	# =========================================================================

	def getLastPosition( self ):
		"""Returns the last saved position."""
		if not self._positionPath or not os.path.exists(self._positionPath):
			return None
		with open(self._positionPath, "r") as f:
			r = f.read()
			try:
				return int(r)
			except ValueError as e:
				return None

	def _savePosition( self, index ):
		"""Saves the given position."""
		if self._positionPath:
			with open(self._positionPath, "w") as f:
				f.write(str(index))



# EOF
