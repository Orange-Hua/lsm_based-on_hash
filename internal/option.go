package internal

import (
	"io"
	"os"
)

// Compression is the per-block compression algorithm to use.
type Compression int

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	nCompression
)

type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
}
type FileSystem interface {
	// Create creates the named file for writing, truncating it if it already
	// exists.
	Create(name string) (File, error)

	// Open opens the named file for reading.
	Open(name string) (File, error)

	// Remove removes the named file or directory.
	Remove(name string) error

	// Rename renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	Rename(oldname, newname string) error

	// MkdirAll creates a directory and all necessary parents. The permission
	// bits perm have the same semantics as in os.MkdirAll. If the directory
	// already exists, MkdirAll does nothing and returns nil.
	MkdirAll(dir string, perm os.FileMode) error

	// Lock locks the given file, creating the file if necessary, and
	// truncating the file if it already exists. The lock is an exclusive lock
	// (a write lock), but locked files should neither be read from nor written
	// to. Such files should have zero size and only exist to co-ordinate
	// ownership across processes.
	//
	// A nil Closer is returned if an error occurred. Otherwise, close that
	// Closer to release the lock.
	//
	// On Linux and OSX, a lock has the same semantics as fcntl(2)'s advisory
	// locks. In particular, closing any other file descriptor for the same
	// file will release the lock prematurely.
	//
	// Attempting to lock a file that is already locked by the current process
	// has undefined behavior.
	//
	// Lock is not yet implemented on other operating systems, and calling it
	// will return an error.
	Lock(name string) (io.Closer, error)

	// List returns a listing of the given directory. The names returned are
	// relative to dir.
	List(dir string) ([]string, error)

	// Stat returns an os.FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)
}
type FilterPolicy interface {
	// Name names the filter policy.
	Name() string

	// AppendFilter appends to dst an encoded filter that holds a set of []byte
	// keys.
	AppendFilter(dst []byte, keys [][]byte) []byte

	// MayContain returns whether the encoded filter may contain given key.
	// False positives are possible, where it returns true for keys not in the
	// original set.
	MayContain(filter, key []byte) bool
}

// Options holds the optional parameters for leveldb's DB implementations.
// These options apply to the DB at large; per-query options are defined by
// the ReadOptions and WriteOptions types.
//
// Options are typically passed to a constructor function as a struct literal.
// The GetXxx methods are used inside the DB implementations; they return the
// default parameter value if the *Options receiver is nil or the field value
// is zero.
//
// Read/Write options:
//   - Comparer
//   - FileSystem
//   - FilterPolicy
//   - MaxOpenFiles
//
// Read options:
//   - VerifyChecksums
//
// Write options:
//   - BlockRestartInterval
//   - BlockSize
//   - Compression
//   - ErrorIfDBExists
//   - WriteBufferSize
type Options struct {
	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	//
	// The default value is 16.
	BlockRestartInterval int

	// BlockSize is the minimum uncompressed size in bytes of each table block.
	//
	// The default value is 4096.
	BlockSize int

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer Comparer

	// Compression defines the per-block compression to use.
	//
	// The default value (DefaultCompression) uses snappy compression.
	Compression Compression

	// ErrorIfDBExists is whether it is an error if the database already exists.
	//
	// The default value is false.
	ErrorIfDBExists bool

	// FileSystem maps file names to byte storage.
	//
	// The default value uses the underlying operating system's file system.
	FileSystem FileSystem

	// FilterPolicy defines a filter algorithm (such as a Bloom filter) that
	// can reduce disk reads for Get calls.
	//
	// One such implementation is bloom.FilterPolicy(10) from the leveldb/bloom
	// package.
	//
	// The default value means to use no filter.
	FilterPolicy FilterPolicy

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// WriteBufferSize is the amount of data to build up in memory (backed by
	// an unsorted log on disk) before converting to a sorted on-disk file.
	//
	// Larger values increase performance, especially during bulk loads. Up to
	// two write buffers may be held in memory at the same time, so you may
	// wish to adjust this parameter to control memory usage. Also, a larger
	// write buffer will result in a longer recovery time the next time the
	// database is opened.
	//
	// The default value is 4MiB.
	WriteBufferSize int

	// VerifyChecksums is whether to verify the per-block checksums in a DB.
	//
	// The default value is false.
	VerifyChecksums bool
}

func (o *Options) GetBlockRestartInterval() int {
	if o == nil || o.BlockRestartInterval <= 0 {
		return 16
	}
	return o.BlockRestartInterval
}

func (o *Options) GetBlockSize() int {
	if o == nil || o.BlockSize <= 0 {
		return 4096
	}
	return o.BlockSize
}

func (o *Options) GetComparer() Comparer {
	if o == nil || o.Comparer == nil {
		return DefaultComparer
	}
	return o.Comparer
}

func (o *Options) GetCompression() Compression {
	if o == nil || o.Compression <= DefaultCompression || o.Compression >= nCompression {
		// Default to SnappyCompression.
		return SnappyCompression
	}
	return o.Compression
	//return DefaultCompression
}

func (o *Options) GetErrorIfDBExists() bool {
	if o == nil {
		return false
	}
	return o.ErrorIfDBExists
}

func (o *Options) GetFileSystem() FileSystem {
	if o == nil {
		return nil
	}
	return o.FileSystem
}

func (o *Options) GetFilterPolicy() FilterPolicy {
	if o == nil {
		return nil
	}
	return o.FilterPolicy
}

func (o *Options) GetMaxOpenFiles() int {
	if o == nil || o.MaxOpenFiles == 0 {
		return 1000
	}
	return o.MaxOpenFiles
}

func (o *Options) GetWriteBufferSize() int {
	if o == nil || o.WriteBufferSize <= 0 {
		return 4 * 1024 * 1024
	}
	return o.WriteBufferSize
}

func (o *Options) GetVerifyChecksums() bool {
	if o == nil {
		return false
	}
	return o.VerifyChecksums
}

// ReadOptions hold the optional per-query parameters for Get and Find
// operations.
//
// Like Options, a nil *ReadOptions is valid and means to use the default
// values.
type ReadOptions struct {
	// No fields so far.
}

// WriteOptions hold the optional per-query parameters for Set and Delete
// operations.
//
// Like Options, a nil *WriteOptions is valid and means to use the default
// values.
type WriteOptions struct {
	// Sync is whether to sync underlying writes from the OS buffer cache
	// through to actual disk, if applicable. Setting Sync can result in
	// slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does
	// not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	//
	// The default value is false.
	Sync bool
}

func (o *WriteOptions) GetSync() bool {
	return o != nil && o.Sync
}
