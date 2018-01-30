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

import os, stat, hashlib, logging, shutil
from .catalogue import CatalogueReader

class Dedup(CatalogueReader):

	STORAGE_PATH = "fstk-dedup-{0}"
	INDEX_PATH   = "fstk-dedup-{0}/last.pos"

	def __init__( self, path, logging=logging ):
		h = hashlib.md5(bytes(path,"UTF-8")).hexdigest()
		CatalogueReader.__init__(self, self.INDEX_PATH.format(h))
		self.logging = logging
		self.dbPath = self.STORAGE_PATH.format(h)
		if not os.path.exists(self.dbPath):
			os.mkdir(self.dbPath)
		# We clean any previous run
		# self.clean()
		# We start reading
		# self.read(path)
		for p in self.listSHA1Paths():
			self.dedup(self.getSHA1Paths(p))

	def listSHA1Paths( self, parent=None, depth=10 ):
		"""Iterates through the SHA-1 paths stored here."""
		parent = parent or self.dbPath
		if os.path.isdir(parent) and depth >= 0:
			for name in os.listdir(parent):
				path = os.path.join(parent, name)
				if path.endswith(".lst"):
					yield path
				else:
					yield from self.listSHA1Paths(path, depth - 1)

	def getSHA1Paths( self, path ):
		if os.path.exists(path):
			with open(path, "rt") as f:
				return [_[:-1] for _ in f.readlines()]
		else:
			return ()

	def getPathForSHA1( self, sha1 ):
		assert self.dbPath
		# We split the SHA1 path in groups of 5
		assert len(sha1) == 40
		assert len(sha1) % 5 == 0
		return "/".join(sha1[o:o+5] for o in range(0,len(sha1), 5))

	def clean( self ):
		pass
		# for p in self.listPaths():
		# 	pass
		# 	#os.unlink(p)

	def onFile( self, path, type, index ):
		if type == "F":
			# We split the SHA1 path in groups of 5
			# We get the file name
			p = os.path.join(self.dbPath, self.getPathForSHA1(self.sha1sum(path))) + ".lst"
			# And the dirname, which we make sure exists
			d = os.path.dirname(p)
			if not os.path.exists(d):
				os.makedirs(d)
			# And we register the fil in the entry
			with open(p, "at") as f:
				f.write(path)
				f.write("\n")
		return True

	def sha1sum( self, path ):
		"""Returns the SHA1 of the given path."""
		with open(path, "rb") as f:
			return hashlib.sha1(f.read()).hexdigest()

	def dedup( self, paths ):
		"""The core deduplication function."""
		if len(paths) <= 1:
			return None
		inodes      = {}
		inodes_path = {}
		path_inode  = {}
		#self.logging.info("Dedup: {0} [1+{1}]".format(paths[0], len(paths[1:])))
		# We iterate through the path, and  get the inode with the oldest
		# mtime
		for p in paths:
			if not os.path.exists(p):
				continue
			s = os.stat(p)
			i = s[stat.ST_INO]
			m = s[stat.ST_MTIME]
			path_inode[p] = i
			if i in inodes:
				inodes[i] = min(inodes[i], m)
			else:
				inodes[i]      = m
				inodes_path[i] = p
		if not inodes:
			return None
		# We take the oldest inode
		sorted_inodes  = sorted(inodes.items(), key=lambda _:_[1])
		original_inode = sorted_inodes[0][0]
		original_path  = inodes_path[original_inode]
		# Now we iterate back on the paths and re-create the symlinks
		to_dedup = []
		for p in paths:
			if p != original_path:
				if path_inode[p] != original_inode:
					to_dedup.append(p)
		if to_dedup:
			print("Dedup: {0} [1+{1}/{2}]".format(paths[0], len(to_dedup), len(paths[1:])))
			for p in to_dedup:
				print(" - {0}".format(p))
				os.unlink(p)
				os.link(original_path, p, follow_symlinks=False)
				self.copyattr(original_path, p)

	def copyattr( self, source, destination, stats=None ):
		"""Copies the attributes from source to destination, (re)using the
		given `stats` info if provided."""
		s_stat = stats or os.lstat(source)
		shutil.copystat(source, destination, follow_symlinks=False)
		os.chown(destination, s_stat[stat.ST_GID], s_stat[stat.ST_UID], follow_symlinks=False)

# EOF - vim: ts=4 sw=4 noet
