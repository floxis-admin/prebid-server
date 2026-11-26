package gdpr

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prebid/go-gdpr/api"
	"github.com/prebid/go-gdpr/vendorlist"
	"github.com/prebid/go-gdpr/vendorlist2"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/metrics"
	"golang.org/x/net/context/ctxhttp"
)

type saveVendors func(uint16, uint16, api.VendorList)
type persistVendors func(specVersion, listVersion uint16, body []byte)
type VendorListFetcher func(ctx context.Context, specVersion uint16, listVersion uint16, metricsEngine metrics.MetricsEngine) (vendorlist.VendorList, error)

// latestSpecVersion is the highest GVL specification version supported. Update this value when
// a new spec version is introduced by the IAB.
const latestSpecVersion = 3

// This file provides the vendorlist-fetching function for Prebid Server.
//
// For more info, see https://github.com/prebid/prebid-server/issues/504
//
// Nothing in this file is exported. Public APIs can be found in gdpr.go

func NewVendorListFetcher(initCtx context.Context, cfg config.GDPR, client *http.Client, metricsEngine metrics.MetricsEngine, urlMaker func(uint16, uint16) string) VendorListFetcher {
	fetcher, _ := newVendorListFetcher(initCtx, cfg, client, metricsEngine, urlMaker)
	return fetcher
}

// newVendorListFetcher also returns a channel closed when the background refresh of a disk-backed
// cache finishes (nil without a cache dir).
func newVendorListFetcher(initCtx context.Context, cfg config.GDPR, client *http.Client, metricsEngine metrics.MetricsEngine, urlMaker func(uint16, uint16) string) (VendorListFetcher, <-chan struct{}) {
	cacheSave, cacheLoad := newVendorListCache()
	known := func(specVersion, listVersion uint16) bool { return cacheLoad(specVersion, listVersion) != nil }

	var disk *vendorListDisk
	var persist persistVendors
	var loadedFromDisk map[uint16]int
	if cfg.VendorListCacheDir != "" {
		disk = &vendorListDisk{dir: cfg.VendorListCacheDir}
		persist = disk.save
		loadedFromDisk = disk.preloadNewest(cacheSave, diskPreloadNewestPerSpec)
		known = func(specVersion, listVersion uint16) bool {
			return cacheLoad(specVersion, listVersion) != nil || disk.has(specVersion, listVersion)
		}
		logger.Infof("Loaded vendor lists %v (spec version: count) from %s", loadedFromDisk, cfg.VendorListCacheDir)
	}

	// Never listen without a spec version's lists: TCF enforcement would drop every bidder on its gdpr=1 traffic.
	blockOnNetwork := false
	for _, v := range preloadedSpecVersions {
		blockOnNetwork = blockOnNetwork || loadedFromDisk[v.specVersion] == 0
	}
	if blockOnNetwork {
		preloadContext, cancel := context.WithTimeout(initCtx, cfg.Timeouts.InitTimeout())
		preloadCache(preloadContext, client, urlMaker, cacheSave, known, persist, metricsEngine)
		cancel()
	}

	var refreshed chan struct{}
	if persist != nil {
		refreshed = make(chan struct{})
		go func() {
			defer close(refreshed)
			refreshContext, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.InitTimeout())
			defer cancel()
			preloadCache(refreshContext, client, urlMaker, cacheSave, known, persist, metricsEngine)
		}()
	}

	saveOneRateLimited := newOccasionalSaver(cfg.Timeouts.ActiveTimeout())
	return func(ctx context.Context, specVersion, listVersion uint16, metricsEngine metrics.MetricsEngine) (vendorlist.VendorList, error) {
		// Attempt To Load From Cache
		if list := cacheLoad(specVersion, listVersion); list != nil {
			return list, nil
		}

		if disk != nil {
			if list := disk.loadOne(specVersion, listVersion, cacheLoad, cacheSave); list != nil {
				return list, nil
			}
		}

		// Attempt To Download
		// - May not add to cache immediately.
		saveOneRateLimited(ctx, client, urlMaker(specVersion, listVersion), cacheSave, persist, metricsEngine)

		// Attempt To Load From Cache Again
		// - May have been added by the call to saveOneRateLimited.
		if list := cacheLoad(specVersion, listVersion); list != nil {
			return list, nil
		}

		// Give Up
		return nil, makeVendorListNotFoundError(specVersion, listVersion)
	}, refreshed
}

func makeVendorListNotFoundError(specVersion, listVersion uint16) error {
	return fmt.Errorf("gdpr vendor list spec version %d list version %d does not exist, or has not been loaded yet. Try again in a few minutes", specVersion, listVersion)
}

// preloadCache saves all the known versions of the vendor list for future use, skipping those
// known already (known may be nil). persist may be nil.
func preloadCache(ctx context.Context, client *http.Client, urlMaker func(uint16, uint16) string, saver saveVendors, known func(uint16, uint16) bool, persist persistVendors, metricsEngine metrics.MetricsEngine) {
	for _, v := range preloadedSpecVersions {
		latestVersion := saveOne(ctx, client, urlMaker(v.specVersion, 0), saver, persist, metricsEngine)

		// Newest first: consent strings overwhelmingly carry recent versions, so a budget that runs
		// out leaves only the oldest missing.
		for i := int(latestVersion) - 1; i >= int(v.firstListVersion); i-- {
			if known != nil && known(v.specVersion, uint16(i)) {
				continue
			}
			saveOne(ctx, client, urlMaker(v.specVersion, uint16(i)), saver, persist, metricsEngine)
		}
	}
}

var preloadedSpecVersions = [2]struct {
	specVersion      uint16
	firstListVersion uint16
}{
	{
		specVersion:      2,
		firstListVersion: 2, // The GVL for TCF2 has no vendors defined in its first version. It's very unlikely to be used, so don't preload it.
	},
	{
		specVersion:      latestSpecVersion,
		firstListVersion: 1,
	},
}

// Make a URL which can be used to fetch a given version of the Global Vendor List. If the version is 0,
// this will fetch the latest version.
func VendorListURLMaker(specVersion, listVersion uint16) string {
	if listVersion == 0 {
		return "https://vendor-list.consensu.org/v" + strconv.Itoa(int(specVersion)) + "/vendor-list.json"
	}
	return "https://vendor-list.consensu.org/v" + strconv.Itoa(int(specVersion)) + "/archives/vendor-list-v" + strconv.Itoa(int(listVersion)) + ".json"
}

// newOccasionalSaver returns a wrapped version of saveOne() which only activates every few minutes.
//
// The goal here is to update quickly when new versions of the VendorList are released, but not wreck
// server performance if a bad CMP starts sending us malformed consent strings that advertize a version
// that doesn't exist yet.
func newOccasionalSaver(timeout time.Duration) func(ctx context.Context, client *http.Client, url string, saver saveVendors, persist persistVendors, metricsEngine metrics.MetricsEngine) {
	lastSaved := &atomic.Value{}
	lastSaved.Store(time.Time{})

	return func(ctx context.Context, client *http.Client, url string, saver saveVendors, persist persistVendors, metricsEngine metrics.MetricsEngine) {
		now := time.Now()
		timeSinceLastSave := now.Sub(lastSaved.Load().(time.Time))

		if timeSinceLastSave.Minutes() > 10 {
			withTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			saveOne(withTimeout, client, url, saver, persist, metricsEngine)
			lastSaved.Store(now)
		}
	}
}

func saveOne(ctx context.Context, client *http.Client, url string, saver saveVendors, persist persistVendors, me metrics.MetricsEngine) uint16 {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logger.Errorf("Failed to build GET %s request. Cookie syncs may be affected: %v", url, err)
		return 0
	}

	resp, err := ctxhttp.Do(ctx, client, req)
	if err != nil {
		logger.Errorf("Error calling GET %s. Cookie syncs may be affected: %v", url, err)
		return 0
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("Error reading response body from GET %s. Cookie syncs may be affected: %v", url, err)
		return 0
	}
	if resp.StatusCode != http.StatusOK {
		logger.Errorf("GET %s returned %d. Cookie syncs may be affected.", url, resp.StatusCode)
		me.RecordGvlListRequest()
		return 0
	}
	me.RecordGvlListRequest()

	var newList api.VendorList
	newList, err = vendorlist2.ParseEagerly(respBody)
	if err != nil {
		logger.Errorf("GET %s returned malformed JSON. Cookie syncs may be affected. Error was %v. Body was %s", url, err, string(respBody))
		return 0
	}

	saver(newList.SpecVersion(), newList.Version(), newList)
	if persist != nil {
		persist(newList.SpecVersion(), newList.Version(), respBody)
	}
	return newList.Version()
}

func newVendorListCache() (save func(specVersion, listVersion uint16, list api.VendorList), load func(specVersion, listVersion uint16) api.VendorList) {
	cache := &sync.Map{}

	save = func(specVersion uint16, listVersion uint16, list api.VendorList) {
		key := fmt.Sprint(specVersion) + "-" + fmt.Sprint(listVersion)
		cache.Store(key, list)
	}

	load = func(specVersion, listVersion uint16) api.VendorList {
		key := fmt.Sprint(specVersion) + "-" + fmt.Sprint(listVersion)
		list, ok := cache.Load(key)
		if ok {
			return list.(vendorlist.VendorList)
		}
		return nil
	}
	return
}
