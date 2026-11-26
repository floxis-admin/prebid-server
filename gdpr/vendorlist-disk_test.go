package gdpr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var vendorList3 = MarshalVendorList(vendorList{
	GVLSpecificationVersion: 3,
	VendorListVersion:       3,
	Vendors:                 map[string]*vendor{"12": {ID: 12, Purposes: []int{4}}},
})

func diskCacheConfig(dir string) config.GDPR {
	cfg := testConfig()
	cfg.VendorListCacheDir = dir
	return cfg
}

var specTwoVendorList = MarshalVendorList(vendorList{
	GVLSpecificationVersion: 2,
	VendorListVersion:       5,
	Vendors:                 map[string]*vendor{"12": {ID: 12, Purposes: []int{1}}},
})

func writeCachedList(t *testing.T, dir, spec, name, body string) {
	require.NoError(t, os.MkdirAll(filepath.Join(dir, spec), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, spec, name), []byte(body), 0o644))
}

func TestDiskCacheServesBeforeTheNetworkAnswers(t *testing.T) {
	defer func(previous int) { diskPreloadNewestPerSpec = previous }(diskPreloadNewestPerSpec)
	diskPreloadNewestPerSpec = 1
	dir := t.TempDir()
	writeCachedList(t, dir, "v2", "vendor-list-v5.json", specTwoVendorList)
	writeCachedList(t, dir, "v3", "vendor-list-v1.json", vendorList1)
	writeCachedList(t, dir, "v3", "vendor-list-v2.json", vendorList2)

	release := make(chan struct{})
	var requestedMu sync.Mutex
	requested := map[string]int{}
	upstream := mockServer(serverSettings{
		vendorListLatestVersion: 3,
		vendorLists:             map[int]map[int]string{3: {1: vendorList1, 2: vendorList2, 3: vendorList3}},
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedMu.Lock()
		requested[r.URL.Query().Get("listversion")]++
		requestedMu.Unlock()
		<-release
		upstream(w, r)
	}))
	defer server.Close()
	m := &metrics.MetricsEngineMock{}
	m.On("RecordGvlListRequest").Maybe()

	type started struct {
		fetcher   VendorListFetcher
		refreshed <-chan struct{}
	}
	done := make(chan started)
	go func() {
		f, r := newVendorListFetcher(context.Background(), diskCacheConfig(dir), server.Client(), m, testURLMaker(server))
		done <- started{f, r}
	}()
	var s started
	select {
	case s = <-done:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("startup waited on the network despite a populated vendor list cache")
	}

	list, err := s.fetcher(context.Background(), 3, 2, m)
	require.NoError(t, err)
	assert.Equal(t, uint16(2), list.Version())
	assert.True(t, list.Vendor(12).Purpose(3))

	list, err = s.fetcher(context.Background(), 3, 1, m)
	require.NoError(t, err, "a version past the startup preload must load from disk, not the network")
	assert.Equal(t, uint16(1), list.Version())
	assert.True(t, list.Vendor(12).Purpose(2))

	close(release)
	<-s.refreshed
	list, err = s.fetcher(context.Background(), 3, 3, m)
	require.NoError(t, err, "background refresh must load versions newer than the snapshot")
	assert.Equal(t, uint16(3), list.Version())
	assert.FileExists(t, filepath.Join(dir, "v3", "vendor-list-v3.json"))
	requestedMu.Lock()
	defer requestedMu.Unlock()
	assert.Zero(t, requested["1"]+requested["2"], "lists already on disk must not be downloaded again")
}

func TestDiskCacheEmptyPreloadsFromNetworkAndPersists(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(mockServer(serverSettings{
		vendorListLatestVersion: 2,
		vendorLists:             map[int]map[int]string{3: {1: vendorList1, 2: vendorList2}},
	})))
	m := &metrics.MetricsEngineMock{}
	m.On("RecordGvlListRequest").Maybe()

	fetcher, refreshed := newVendorListFetcher(context.Background(), diskCacheConfig(dir), server.Client(), m, testURLMaker(server))
	list, err := fetcher(context.Background(), 3, 1, m)
	require.NoError(t, err, "an empty cache dir must still preload before serving")
	assert.Equal(t, uint16(1), list.Version())
	<-refreshed
	server.Close()
	assert.FileExists(t, filepath.Join(dir, "v3", "vendor-list-v1.json"))
	assert.FileExists(t, filepath.Join(dir, "v3", "vendor-list-v2.json"))

	restarted, refreshed := newVendorListFetcher(context.Background(), diskCacheConfig(dir), server.Client(), m, testURLMaker(server))
	for _, version := range []uint16{1, 2} {
		list, err := restarted(context.Background(), 3, version, m)
		require.NoError(t, err, "a restart with the network down must serve from disk")
		assert.Equal(t, version, list.Version())
	}
	<-refreshed
}

// regression: 9f48c67 listened with spec-2 lists on disk and no spec-3 lists loaded.
func TestDiskCacheMissingASpecVersionPreloadsItFromNetwork(t *testing.T) {
	dir := t.TempDir()
	writeCachedList(t, dir, "v2", "vendor-list-v5.json", specTwoVendorList)
	server := httptest.NewServer(http.HandlerFunc(mockServer(serverSettings{
		vendorListLatestVersion: 2,
		vendorLists:             map[int]map[int]string{3: {1: vendorList1, 2: vendorList2}},
	})))
	defer server.Close()
	m := &metrics.MetricsEngineMock{}
	m.On("RecordGvlListRequest").Maybe()

	fetcher, refreshed := newVendorListFetcher(context.Background(), diskCacheConfig(dir), server.Client(), m, testURLMaker(server))
	for _, version := range []uint16{2, 1} {
		list, err := fetcher(context.Background(), 3, version, m)
		require.NoError(t, err, "spec 3 must be loaded before serving when the cache dir holds none of it")
		assert.Equal(t, version, list.Version())
	}
	<-refreshed
}
