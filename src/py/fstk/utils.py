import dbm

class FS:

	def inode( self, path ):
		s     = os.lstat(source)
		inode = s[stat.ST_INO]

	def hardlink( self, source, destination ):
		"""Copies the file/directory as a hard link. Return True if
		a hard link was detected."""
		# Otherwise if the inode is already there, then we can
		# simply hardlink it
		s     = os.lstat(source)
		inode = s[stat.ST_INO]
		mode  = s[stat.ST_MODE]
		if stat.S_ISDIR(mode) or os.path.exists(destination):
			# Directories can't have hard links
			return False
		original_path = self.getInodePath(inode)
		if original_path:
			logging.info("Hard linking file: {0}".format(destination))
			link_source = os.path.join(self.base, original_path)
			os.link(link_source, destination, follow_symlinks=False)
			self.copyattr(source, destination)
			return True
		else:
			return False

# -----------------------------------------------------------------------------
#
# KEY VALUE STORAGE
#
# -----------------------------------------------------------------------------

class KeyValueStorage(object):
	"""Abstraction over a (K,V) storage. Uses DBM by default."""

	def __init__( self, path ):
		self.db     = None
		self.path   = path
		self.open(path)

	def set( self, key, value ):
		self.db[key] = value
		return value

	def has( self, key ):
		return key in self.db

	def get( self, key ):
		return self.db[key]

	def open( self, path ):
		self.close()
		if not self.db:
			self.db = dbm.open(path, "c")
		return self

	def close( self ):
		if self.db:
			self.db.close()
			self.db = None
		return self

	def sync( self, index ):
		if hasattr(self.db, "sync"):
			self.db.sync()

# EOF
