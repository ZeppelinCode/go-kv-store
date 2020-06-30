package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ZeppelinCode/go-kv-store/src/service"
)

type serverHandler func(http.ResponseWriter, *http.Request)

type Server struct {
	Ds *service.DocService
}

func (s *Server) ServeWebPage() {
	r := gin.Default()

	r.GET("/documents", func(c *gin.Context) {
		storedDocs := s.Ds.ListDocuments()
		c.JSON(http.StatusOK, storedDocs)
	})

	r.GET("/documents/:id", func(c *gin.Context) {
		id := c.Param("id")
		docEntity, err := s.Ds.GetDocument(id)
		if err != nil {
			c.String(http.StatusNotFound, err.Error())
		}
		c.JSON(http.StatusOK, *docEntity)
	})

	r.POST("/documents", func(c *gin.Context) {
		var creationRequest service.DocumentCreationRequest
		if err := c.ShouldBindJSON(&creationRequest); err == nil {
			err = s.Ds.CreateDocument(&creationRequest)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			c.Status(http.StatusCreated)
		} else {
			c.String(http.StatusBadRequest, err.Error())
		}
	})

	r.PUT("/documents/:id", func(c *gin.Context) {
		id := c.Param("id")
		var updateRequest service.DocumentUpdateRequest
		if err := c.ShouldBindJSON(&updateRequest); err == nil {
			err = s.Ds.UpdateDocument(id, &updateRequest)
			if err != nil {
				c.String(http.StatusInternalServerError, err.Error())
				return
			}
			c.Status(http.StatusNoContent)
		} else {
			c.String(http.StatusBadRequest, err.Error())
		}
	})

	r.DELETE("/documents/:id", func(c *gin.Context) {
		id := c.Param("id")
		err := s.Ds.DeleteDocument(id)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		c.Status(http.StatusNoContent)
	})

	r.Run(":3000")
}
