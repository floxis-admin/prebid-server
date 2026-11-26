package gdpr

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/prebid/go-gdpr/api"
	"github.com/prebid/go-gdpr/vendorlist2"
	"github.com/prebid/prebid-server/v4/logger"
)

// diskPreloadNewestPerSpec bounds startup parsing: all ~400 archived lists take seconds, while
// consent strings overwhelmingly carry recent versions. Older ones load from disk on first use.
var diskPreloadNewestPerSpec = 16

// vendorListDisk persists every fetched vendor list so a restart preloads from disk instead of
// fetching hundreds of lists before the server listens. Archived list versions never change.
type vendorListDisk struct {
	dir     string
	loading sync.Map
}

func (d *vendorListDisk) path(specVersion, listVersion uint16) string {
	return filepath.Join(d.dir, "v"+strconv.Itoa(int(specVersion)), "vendor-list-v"+strconv.Itoa(int(listVersion))+".json")
}

func (d *vendorListDisk) has(specVersion, listVersion uint16) bool {
	_, err := os.Stat(d.path(specVersion, listVersion))
	return err == nil
}

func (d *vendorListDisk) save(specVersion, listVersion uint16, body []byte) {
	path := d.path(specVersion, listVersion)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Errorf("Failed to create vendor list cache dir %s: %v", dir, err)
		return
	}
	tmp, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		logger.Errorf("Failed to write vendor list cache in %s: %v", dir, err)
		return
	}
	_, err = tmp.Write(body)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		os.Remove(tmp.Name())
		logger.Errorf("Failed to write vendor list cache %s: %v", path, err)
	}
}

func (d *vendorListDisk) parse(path string) api.VendorList {
	body, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Errorf("Failed to read cached vendor list %s: %v", path, err)
		}
		return nil
	}
	list, err := vendorlist2.ParseEagerly(body)
	if err != nil {
		logger.Errorf("Ignoring malformed cached vendor list %s: %v", path, err)
		return nil
	}
	return list
}

// preloadNewest parses the newest persisted lists of each spec version into saver and returns how
// many it loaded per spec version.
func (d *vendorListDisk) preloadNewest(saver saveVendors, perSpec int) map[uint16]int {
	var paths []string
	specDirs, _ := filepath.Glob(filepath.Join(d.dir, "v*"))
	for _, specDir := range specDirs {
		files, _ := filepath.Glob(filepath.Join(specDir, "vendor-list-v*.json"))
		versionOf := func(path string) int {
			v, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "vendor-list-v"), ".json"))
			return v
		}
		slices.SortFunc(files, func(a, b string) int { return versionOf(b) - versionOf(a) })
		paths = append(paths, files[:min(perSpec, len(files))]...)
	}

	loaded := map[uint16]int{}
	var loadedMu sync.Mutex
	var wg sync.WaitGroup
	work := make(chan string)
	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range work {
				if list := d.parse(path); list != nil {
					saver(list.SpecVersion(), list.Version(), list)
					loadedMu.Lock()
					loaded[list.SpecVersion()]++
					loadedMu.Unlock()
				}
			}
		}()
	}
	for _, path := range paths {
		work <- path
	}
	close(work)
	wg.Wait()
	return loaded
}

// loadOne parses a persisted list into saver once, however many requests ask for it at the same time.
func (d *vendorListDisk) loadOne(specVersion, listVersion uint16, load func(uint16, uint16) api.VendorList, saver saveVendors) api.VendorList {
	lock, _ := d.loading.LoadOrStore(d.path(specVersion, listVersion), &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()
	if list := load(specVersion, listVersion); list != nil {
		return list
	}
	list := d.parse(d.path(specVersion, listVersion))
	if list == nil || list.SpecVersion() != specVersion || list.Version() != listVersion {
		return nil
	}
	saver(specVersion, listVersion, list)
	return list
}
