package sync

import (
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

	return router
}
