package storage

const (
	blockTrailerLen = 5
	footerLen       = 48

	magic = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"

	// The block type gives the per-block compression format.
	// These constants are part of the file format and should not be changed.
	// They are different from the db.Compression constants because the latter
	// are designed so that the zero value of the db.Compression type means to
	// use the default compression (which is snappy).
	noCompressionBlockType     = 0
	snappyCompressionBlockType = 1
)
