package datasource

import (
	"github.com/gorilla/mux"
)

// NewRouter creates and configures a mux router
func NewRouter(service *Service) *mux.Router {

	// API framework routes
	router := mux.NewRouter().StrictSlash(true)

	// Application
	router.HandleFunc("/v1/file", service.Get).Methods("Get")
	router.HandleFunc("/v1/file", service.Upload).Methods("POST").Queries("action", "upload")
	router.HandleFunc("/v1/file", service.Transfer).Methods("POST").Queries("action", "transfer")

	return router
}
