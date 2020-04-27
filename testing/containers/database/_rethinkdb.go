package database

import (
	"fmt"
	"log"

	"github.com/dchest/uniuri"
	dockertest "gopkg.in/ory-am/dockertest.v3"

	"github.com/scraly/go.pkg/testing/containers"
)

var (
	// RethinkDBVersion defines version to use
	RethinkDBVersion = "latest"
)

// rethinkDBContainer represents database container handler
type rethinkDBContainer struct {
	Name             string
	ConnectionString string
	Password         string
	DatabaseName     string
	DatabaseUser     string
	pool             *dockertest.Pool
	resource         *dockertest.Resource
}

// newRethinkDBContainer initialize a RethinkDB server in a docker container
func newRethinkDBContainer(pool *dockertest.Pool) *rethinkDBContainer {

	var (
		databaseName = fmt.Sprintf("test-%s", uniuri.NewLen(8))
		databaseUser = fmt.Sprintf("user-%s", uniuri.NewLen(8))
		password     = uniuri.NewLen(32)
	)

	// Initialize a PostgreSQL server
	resource, err := pool.Run("rethinkdb", RethinkDBVersion, []string{""})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// Retrieve container name
	containerName := containers.GetName(resource)

	// Return container information
	return &rethinkDBContainer{
		Name:         containerName,
		Password:     password,
		DatabaseName: databaseName,
		DatabaseUser: databaseUser,
		pool:         pool,
		resource:     resource,
	}
}

// -------------------------------------------------------------------

// Close the container
func (container *rethinkDBContainer) Close() error {
	log.Printf("Postgres (%v): shutting down", container.Name)
	return container.pool.Purge(container.resource)
}
