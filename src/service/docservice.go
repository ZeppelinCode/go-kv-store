package service

import (
	"github.com/ZeppelinCode/go-kv-store/src/dbs"
)

type DocService struct {
	db *dbs.Database
}

type DocumentSummaryEntity struct {
	Name string `json:"name"`
	Link string `json:"link"`
}

type DocumentEntity struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

type DocumentCreationRequest struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

type DocumentUpdateRequest struct {
	Payload string `json:"payload"`
}

func NewDocService(db *dbs.Database) *DocService {
	return &DocService{db}
}

func (ds *DocService) ListDocuments() []DocumentSummaryEntity {
	docs := ds.db.StoredDocuments()
	entities := make([]DocumentSummaryEntity, len(docs))
	for i, doc := range docs {
		entities[i] = DocumentSummaryEntity{
			Name: doc,
			Link: "http://localhost:3000/documents/" + doc,
		}
	}
	return entities
}

func (ds *DocService) GetDocument(name string) (*DocumentEntity, error) {
	doc, err := ds.db.RetrieveDocument(name)
	if err != nil {
		return nil, err
	}

	return &DocumentEntity{name, doc.String()}, nil
}

func (ds *DocService) CreateDocument(dcr *DocumentCreationRequest) error {
	return ds.db.StoreNewDocument(dcr.Name, dcr.Payload)
}

func (ds *DocService) UpdateDocument(name string, dcr *DocumentUpdateRequest) error {
	return ds.db.UpdateDocument(name, dcr.Payload)
}

func (ds *DocService) DeleteDocument(name string) error {
	return ds.db.DeleteDocument(name)
}
