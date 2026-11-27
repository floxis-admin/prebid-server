package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/prebid/prebid-server/v4/config"
	"github.com/prebid/prebid-server/v4/logger"
	"github.com/prebid/prebid-server/v4/metrics"
	metricsconfig "github.com/prebid/prebid-server/v4/metrics/config"
)

// Listen blocks forever, serving PBS requests on the given port. This will block forever, until the process is shut down.
func Listen(cfg *config.Configuration, handler http.Handler, adminHandler http.Handler, metrics *metricsconfig.DetailedMetricsEngine) (err error) {
	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, syscall.SIGTERM, syscall.SIGINT)

	// Run the servers. Fan any process-stopper signals out to each server for graceful shutdowns.
	stopAdmin := make(chan os.Signal)
	stopMain := make(chan os.Signal)
	stopPrometheus := make(chan os.Signal)
	stopChannels := []chan<- os.Signal{stopMain}
	done := make(chan struct{})

	if cfg.UnixSocketEnable && len(cfg.UnixSocketName) > 0 { // start the unix_socket server if config enable-it.
		var (
			socketListener net.Listener
			mainServer     = newSocketServer(cfg, handler)
		)
		go shutdownAfterSignals(mainServer, stopMain, done)
		if socketListener, err = newUnixListener(mainServer.Addr, metrics); err != nil {
			logger.Errorf("Error listening for Unix-Socket connections on path %s: %v for socket server", mainServer.Addr, err)
			return
		}
		go runServer(mainServer, "UnixSocket", socketListener)
	} else { // start the TCP server
		var (
			mainListener net.Listener
			draining     atomic.Bool
		)
		if cfg.SocketReusePort {
			handler = closeWhileDraining(handler, &draining)
		}
		mainServer := newMainServer(cfg, handler)
		if mainListener, err = newTCPListener(mainServer.Addr, cfg.SocketReusePort, metrics); err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for main server", mainServer.Addr, err)
			return
		}
		if cfg.SocketReusePort {
			go handOverAfterSignals(mainServer, mainListener, &draining, stopMain, done)
		} else {
			go shutdownAfterSignals(mainServer, stopMain, done)
		}
		go runServer(mainServer, "Main", mainListener)
	}

	if cfg.Admin.Enabled {
		stopChannels = append(stopChannels, stopAdmin)
		adminServer := newAdminServer(cfg, adminHandler)
		go shutdownAfterSignals(adminServer, stopAdmin, done)

		var adminListener net.Listener
		if adminListener, err = newTCPListener(adminServer.Addr, cfg.SocketReusePort, nil); err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for admin server", adminServer.Addr, err)
			return
		}
		go runServer(adminServer, "Admin", adminListener)
	}

	if cfg.Metrics.Prometheus.Port != 0 {
		var (
			prometheusListener net.Listener
			prometheusServer   = newPrometheusServer(cfg, metrics)
		)
		stopChannels = append(stopChannels, stopPrometheus)
		go shutdownAfterSignals(prometheusServer, stopPrometheus, done)
		if prometheusListener, err = newTCPListener(prometheusServer.Addr, cfg.SocketReusePort, nil); err != nil {
			logger.Errorf("Error listening for TCP connections on %s: %v for prometheus server", prometheusServer.Addr, err)
			return
		}

		go runServer(prometheusServer, "Prometheus", prometheusListener)
	}

	// Every listener is bound, so connections queue for the servers from here on.
	notifyReady()

	wait(stopSignals, done, stopChannels...)

	return
}

func newAdminServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    cfg.Host + ":" + strconv.Itoa(cfg.AdminPort),
		Handler: handler,
	}
}

func newMainServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	serverHandler := getCompressionEnabledHandler(handler, cfg.Compression.Response)

	return &http.Server{
		Addr:         cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Handler:      serverHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

}

func newSocketServer(cfg *config.Configuration, handler http.Handler) *http.Server {
	serverHandler := getCompressionEnabledHandler(handler, cfg.Compression.Response)

	return &http.Server{
		Addr:         cfg.UnixSocketName,
		Handler:      serverHandler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
}

func getCompressionEnabledHandler(h http.Handler, compressionInfo config.CompressionInfo) http.Handler {
	if compressionInfo.GZIP {
		h = gziphandler.GzipHandler(h)
	}
	return h
}

func runServer(server *http.Server, name string, listener net.Listener) (err error) {
	if server == nil {
		err = fmt.Errorf(">> Server is a nil_ptr.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	} else if listener == nil {
		err = fmt.Errorf(">> Listener is a nil.")
		logger.Errorf("%s server quit with error: %v", name, err)
		return
	}

	logger.Infof("%s server starting on: %s", name, server.Addr)
	if err = server.Serve(listener); err != nil {
		logger.Errorf("%s server quit with error: %v", name, err)
	}
	return
}

func newTCPListener(address string, reusePort bool, metrics metrics.MetricsEngine) (net.Listener, error) {
	var lc net.ListenConfig
	if reusePort {
		lc.Control = setReusePort
	}
	ln, err := lc.Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("Error listening for TCP connections on %s: %v", address, err)
	}

	// This cast is in Go's core libs as Server.ListenAndServe(), so it _should_ be safe, but just in case it changes in a future version...
	if casted, ok := ln.(*net.TCPListener); ok {
		ln = &tcpKeepAliveListener{casted}
	} else {
		logger.Warnf("net.Listen(\"tcp\", \"addr\") didn't return a TCPListener as it did in Go 1.9. Things will probably work fine... but this should be investigated.")
	}

	if metrics != nil {
		ln = &monitorableListener{ln, metrics}
	}

	return ln, nil
}

func newUnixListener(address string, metrics metrics.MetricsEngine) (net.Listener, error) {
	ln, err := net.Listen("unix", address)
	if err != nil {
		return nil, fmt.Errorf("Error listening for Unix-Socket connections on path %s: %v", address, err)
	}

	if casted, ok := ln.(*net.UnixListener); ok {
		ln = &unixListener{casted}
	} else {
		logger.Warnf("net.Listen(\"unix\", \"addr\") didn't return an UnixListener.")
	}

	if metrics != nil {
		ln = &monitorableListener{ln, metrics}
	}

	return ln, nil
}

func wait(inbound <-chan os.Signal, done <-chan struct{}, outbound ...chan<- os.Signal) {
	sig := <-inbound

	for i := 0; i < len(outbound); i++ {
		go sendSignal(outbound[i], sig)
	}

	for i := 0; i < len(outbound); i++ {
		<-done
	}
}

func shutdownAfterSignals(server *http.Server, stopper <-chan os.Signal, done chan<- struct{}) {
	sig := <-stopper

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var s struct{}
	logger.Infof("Stopping %s because of signal: %s", server.Addr, sig.String())
	if err := server.Shutdown(ctx); err != nil {
		logger.Errorf("Failed to shutdown %s: %v", server.Addr, err)
	}
	done <- s
}

// handOverGrace is how long a process sharing its port keeps serving the connections it already
// accepted after it stops accepting: http.Server.Shutdown drops a request it reads once it began.
const handOverGrace = time.Second

// handOverAfterSignals stops accepting first, so the process sharing the port takes every new
// connection (and, with net.ipv4.tcp_migrate_req=1, this listener's queue), then drains.
func handOverAfterSignals(server *http.Server, ln net.Listener, draining *atomic.Bool, stopper <-chan os.Signal, done chan<- struct{}) {
	sig := <-stopper
	logger.Infof("Handing over %s because of signal: %s", server.Addr, sig.String())
	draining.Store(true)
	if err := ln.Close(); err != nil {
		logger.Errorf("Failed to close the listener on %s: %v", server.Addr, err)
	}
	time.Sleep(handOverGrace)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Errorf("Failed to shutdown %s: %v", server.Addr, err)
	}
	done <- struct{}{}
}

// closeWhileDraining asks the client to stop reusing a connection once this process hands over,
// so no request lands on one Shutdown is closing.
func closeWhileDraining(h http.Handler, draining *atomic.Bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			w.Header().Set("Connection", "close")
		}
		h.ServeHTTP(w, r)
	})
}

func sendSignal(to chan<- os.Signal, sig os.Signal) {
	to <- sig
}

// notifyReady tells systemd (Type=notify) the servers are listening, so a unit that starts a
// replacement before stopping the old one knows when the replacement can take connections.
func notifyReady() {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return
	}
	if addr[0] == '@' {
		addr = "\x00" + addr[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		logger.Errorf("sd_notify: %v", err)
		return
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("READY=1")); err != nil {
		logger.Errorf("sd_notify: %v", err)
	}
}
