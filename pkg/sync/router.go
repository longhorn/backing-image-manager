package sync

import (
	"net/http/pprof"

	"github.com/gorilla/mux"
)

// NewRouter creates and configures a mux router
func NewRouter(service *Service) *mux.Router {
	router := mux.NewRouter().StrictSlash(true).UseEncodedPath()

	router.HandleFunc("/v1/files", service.List).Methods("GET")

	// Operate a file
	router.HandleFunc("/v1/files/{id}", service.Get).Methods("GET")
	router.HandleFunc("/v1/files/{id}", service.Delete).Methods("DELETE")
	router.HandleFunc("/v1/files/{id}", service.Forget).Methods("POST").Queries("action", "forget")
	router.HandleFunc("/v1/files/{id}", service.SendToPeer).Methods("POST").Queries("action", "sendToPeer")
	router.HandleFunc("/v1/files/{id}/download", service.DownloadToDst).Methods("GET")

	// Launch a new file
	router.HandleFunc("/v1/files", service.Fetch).Methods("POST").Queries("action", "fetch")
	router.HandleFunc("/v1/files", service.DownloadFromURL).Methods("POST").Queries("action", "downloadFromURL")
	router.HandleFunc("/v1/files", service.UploadFromRequest).Methods("POST").Queries("action", "upload")
	router.HandleFunc("/v1/files", service.ReceiveFromPeer).Methods("POST").Queries("action", "receiveFromPeer")

	router.HandleFunc("/debug/pprof/", pprof.Index).Methods("GET")
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline).Methods("GET")
	router.HandleFunc("/debug/pprof/profile", pprof.Profile).Methods("GET")
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol).Methods("GET")
	router.HandleFunc("/debug/pprof/trace", pprof.Trace).Methods("GET")
	router.Handle("/debug/pprof/allocs", pprof.Handler("allocs")).Methods("GET")
	router.Handle("/debug/pprof/block", pprof.Handler("block")).Methods("GET")
	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine")).Methods("GET")
	router.Handle("/debug/pprof/heap", pprof.Handler("heap")).Methods("GET")
	router.Handle("/debug/pprof/mutex", pprof.Handler("mutex")).Methods("GET")
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate")).Methods("GET")

	return router
}
