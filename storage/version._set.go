package storage

import (
	"fmt"
	"io"
	"kv/internal"
	"kv/storage/record"
	"log"
	"sync"

	"os"
)

type VersionSet struct {
	dummyVersion Version
	rwMutex      sync.RWMutex
}

// 从manifest加载各个ve,将ve的各个level上的新增，删除文件，合并到一个bulkversionEdit，中
// 合并到一处，然后利用这些增量，从无新构造出一个新的version
func (s *Storage) init(dirname string) error {

	//读当前manifest
	var (
		current internal.File
		err     error
	)
	if current, err = s.fs.Open(dbFilename(dirname, fileTypeCurrent, 0)); os.IsNotExist(err) {
		return s.init0()
	} else if err != nil {
		return fmt.Errorf("leveldb: could not open CURRENT file for DB %q: %v", dirname, err)
	}

	defer current.Close()

	stat, err := current.Stat()
	if err != nil {
		return err
	}
	n := stat.Size()
	if n == 0 {
		return fmt.Errorf("leveldb: CURRENT file for DB %q is empty", dirname)
	}

	b := make([]byte, n)
	_, err = current.ReadAt(b, 0) //这个是current文件，不是manifest文件
	if err != nil {
		return err
	}

	if b[n-1] != '\n' {
		return fmt.Errorf("leveldb: CURRENT file for DB %q is malformed", dirname)
	}
	b = b[:n-1]
	//读进来manifest,从manifest读versionEdit
	var bve bulkVersionEdit
	manifest, err := s.fs.Open(dirname + string(os.PathSeparator) + string(b))
	if err != nil {
		return fmt.Errorf("leveldb: could not open manifest file %q for DB %q: %v", b, dirname, err)
	}

	defer manifest.Close()
	rr := record.NewReader(manifest) //和wal复用同一个reader
	for {
		r, err := rr.Next() //不断读取下一个record
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		var ve VersionEdit
		err = ve.decode(r) //从manifest的record中解析ve
		if err != nil {
			return err
		}
		//ve从manifest加载比较器，vs从用户设置中加载比较器
		if ve.comparatorName != "" {
			if ve.comparatorName != s.cmp.Name() {
				return fmt.Errorf("leveldb: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from db.Options %q",
					b, dirname, ve.comparatorName, s.cmp.Name())
			}
		}

		bve.accumulate(&ve)
		if ve.logNumber != 0 {
			s.logNumber = ve.logNumber
		}
		if ve.prelogNumber != 0 {
			s.prevLogNumber = ve.prelogNumber
		}

		if ve.nextFileNumber != 0 {
			s.nextFileNumber.Store(ve.nextFileNumber)
		}
		if ve.lastSequence != 0 {
			s.lastSequence = ve.lastSequence
		}
	}
	//todo 以后删除
	s.logNumber = 1
	if s.logNumber == 0 || s.nextFileNumber.Load() == 0 {
		if s.nextFileNumber.Load() == 2 {
			// We have a freshly created DB.
		} else {
			return fmt.Errorf("leveldb: incomplete manifest file %q for DB %q", b, dirname)
		}
	}
	s.markFileNumUsed(s.logNumber)
	s.markFileNumUsed(s.prevLogNumber)
	s.manifestFileNumber = s.GetNextFileNumAndMarkUsed()
	//利用manifest中的各个versionEdit增量加载更新了versionSet,并创建新版本
	newVersion, err := bve.apply(nil, s.cmp)
	if err != nil {
		return err
	}
	s.vs.append(newVersion)
	return nil

}

// manifest是开启的第一个文件
func (s *Storage) init0() (retErr error) {
	const manifestFileNum = 1
	ve := VersionEdit{
		comparatorName: s.cmp.Name(),
		nextFileNumber: manifestFileNum + 1,
	}
	manifestFilename := dbFilename(s.GetDir(), fileTypeManifest, manifestFileNum)
	f, err := s.GetFileSystem().Create(manifestFilename)
	if err != nil {
		return fmt.Errorf("leveldb: could not create %q: %v", manifestFilename, err)
	}
	s.markFileNumUsed(manifestFileNum)
	//s.markFileNumUsed(4)
	defer func() {
		if retErr != nil {
			s.GetFileSystem().Remove(manifestFilename)
		}
	}()
	defer f.Close()

	recWriter := record.NewWriter(f)
	defer recWriter.Close()
	w, err := recWriter.Next()
	if err != nil {
		return err
	}
	err = ve.encode(w)
	if err != nil {
		return err
	}
	err = recWriter.Close()
	if err != nil {
		return err
	}
	s.manifestFileNumber = s.GetNextFileNumAndMarkUsed()
	//s.manifest = recWriter
	log.Printf("kv/storage/version_set.go/init0():执行创建manifest\r\n")
	return setCurrentFile(manifestFileNum, s.dir, s.fs)
}

/*
进一步初始化versionEdit,并利用versionEdit生成version
*/

func (vs *VersionSet) Append(v *Version) {
	vs.append(v)
}
func (vs *VersionSet) append(v *Version) {
	vs.rwMutex.Lock()
	defer vs.rwMutex.Unlock()
	if v.prev != nil || v.next != nil {
		panic("leveldb: version linked list is inconsistent")
	}
	v.prev = vs.dummyVersion.prev
	if v.prev != nil {
		v.prev.next = v
	}
	v.next = &vs.dummyVersion
	v.next.prev = v
}

func (vs *VersionSet) currentVersion() *Version {
	vs.rwMutex.RLock()
	defer vs.rwMutex.RUnlock()

	return vs.dummyVersion.prev

}

func (vs *VersionSet) refCurrentVersion() *Version {
	current := vs.currentVersion()
	if current == nil {
		return nil
	}
	for {
		if current.addRef() > 0 {
			break
		} else {
			current = vs.currentVersion()
		}

	}

	return current
}
