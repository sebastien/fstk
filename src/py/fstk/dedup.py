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
		self.clean()
		# We start reading
		self.read(path)
		# for p in self.listPaths():
		# 	with open(p,"rt") as f:
		# 		paths = [_[:-1] for _ in f.readlines()]
		# 		self.dedup(paths)

	def listPaths( self ):
		if os.path.exists(self.dbPath):
			for p in os.listdir(self.dbPath):
				p = os.path.join(self.dbPath, p)
				if os.path.exists(p):
					yield p

	def clean( self ):
		for p in self.listPaths():
			pass
			#os.unlink(p)

	def onFile( self, path, type, index ):
		if type == "F":
			h = self.sha1(path)
			# TODO: We should split the file name instead so that
			# we create subfolders 3/4/REST
			p = os.path.join(self.dbPath, h) + ".lst"
			with open(p, "at") as f:
				f.write(path)
				f.write("\n")
		return True

	def sha1( self, path ):
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
		print("Dedup: {0} [1+{1}]".format(paths[0], len(paths[1:])))
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
		for p in paths:
			if p != original_path:
				if path_inode[p] != original_inode:
					print ("Hardlink {0}→{1}".format(original_path, p))
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
