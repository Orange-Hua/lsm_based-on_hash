package storage

import (
	"io"
	"kv/internal"
	"os"
)

type defFS struct{}

func newFileSystem() internal.FileSystem {
	return defFS{}
}
func (defFS) Create(name string) (internal.File, error) {
	return os.Create(name)
}

func (defFS) Open(name string) (internal.File, error) {
	return os.Open(name)
}

func (defFS) Remove(name string) error {
	return os.Remove(name)
}

func (defFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (defFS) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
}

func (defFS) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

func (defFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (defFS) Lock(name string) (io.Closer, error) {
	return nil, nil
}
