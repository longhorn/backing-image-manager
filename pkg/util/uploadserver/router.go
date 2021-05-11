package uploadserver

import "github.com/gorilla/mux"

// NewRouter creates and configures a mux router
func NewRouter(server *UploadServer) *mux.Router {

	// API framework routes
	router := mux.NewRouter().StrictSlash(true)

	// Application
	router.HandleFunc("/v1-bi-upload/start", server.start).Methods("POST")
	router.HandleFunc("/v1-bi-upload/prepareChunk", server.prepareChunk).Methods("POST")
	router.HandleFunc("/v1-bi-upload/uploadChunk", server.uploadChunk).Methods("POST")
	router.HandleFunc("/v1-bi-upload/coalesceChunk", server.coalesceChunk).Methods("POST")
	router.HandleFunc("/v1-bi-upload/close", server.close).Methods("POST")

	return router
}
