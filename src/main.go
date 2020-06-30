package main

import (
	"github.com/ZeppelinCode/go-kv-store/src/dbs"
	"github.com/ZeppelinCode/go-kv-store/src/dbs/fsint"
	"github.com/ZeppelinCode/go-kv-store/src/server"
	"github.com/ZeppelinCode/go-kv-store/src/service"
)

func main() {
	positioner := fsint.NewPersistentDocumentPositioner("dbstore")
	db, _ := dbs.NewDatabase("dbstore", positioner)
	docService := service.NewDocService(db)
	s := server.Server{docService}
	s.ServeWebPage()
}
