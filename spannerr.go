// Package spannerr (pronounced Spanner R, or Spanner-er) provides session management and
// a simple interface for Google Cloud Spanner's REST API.
// If you are not on running your services on Google App Engine, you should just use the
// official Cloud Spanner (gRPC) client: https://godoc.org/cloud.google.com/go/spanner
package spannerr

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	spanner "google.golang.org/api/spanner/v1"
	"google.golang.org/appengine"
)

type (
	// Client allows users to manage sessions on Google Cloud Spanner.
	Client interface {
		// AcquireSession will pull an existing session from the local cache. If the session
		// cache is not full, it will create a new session and put it in the cache.
		// Users must pass the Session to ReleaseSession when work is complete.
		AcquireSession(ctx context.Context) (Session, error)
		// ReleaseSession will make the session available in the cache again. Call this after
		// first acquiring a session.
		ReleaseSession(context.Context, Session)

		// Close will attempt to end all existing sessions. If you have shutdown hooks
		// available for your instance type, call this then.
		// If you do not have shutdown hooks, the sessions made will be closed automatically
		// after one hour of idle time: https://cloud.google.com/spanner/docs/sessions
		Close(context.Context) error
	}

	// Session represents a live session on Google Cloud Spanner.
	Session interface {
		// Commit: Commits a transaction. The request includes the mutations to be applied to
		// rows in the database.
		// This function wraps https://godoc.org/google.golang.org/api/spanner/v1#ProjectsInstancesDatabasesSessionsService.Commit
		Commit(ctx context.Context, mutations []*spanner.Mutation, opts *spanner.TransactionOptions) (*spanner.CommitResponse, error)
		// ExecuteSql: Executes an SQL query, returning all rows in a single reply.
		// This function wraps https://godoc.org/google.golang.org/api/spanner/v1#ProjectsInstancesDatabasesSessionsExecuteSqlCall
		ExecuteSQL(ctx context.Context, params []*Param, sql, queryMode string) (*spanner.ResultSet, error)
		// Name returns the session identifier.
		Name() string
	}

	// Param contains the information required to pass a parameter to a Cloud Spanner query.
	Param struct {
		Name  string
		Value interface{}
		// Type will be used to populate the spanner.Type.Code field. More details
		// can be found here: https://godoc.org/google.golang.org/api/spanner/v1#Type
		Type  string
	}

	session struct {
		name string
		sess *spanner.ProjectsInstancesDatabasesSessionsService
	}

	client struct {
		smu      sync.Mutex
		sessions map[string]bool

		conn        string
		maxSessions int
	}
)

// NewClient returns a new Client implementation.
func NewClient(project, instances, database string, maxSessions int) Client {
	return &client{
		conn:        "projects/" + project + "/instances/" + instances + "/databases/" + database,
		maxSessions: maxSessions,
		sessions:    map[string]bool{},
	}
}

func (c *client) AcquireSession(ctx context.Context) (Session, error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	// fill the buffer first
	if len(c.sessions) < c.maxSessions {
		sess, err := c.newSession(ctx)
		if err != nil {
			return nil, err
		}
		c.sessions[sess.Name()] = true
		return sess, nil
	}
	// range over existing sessions until we find a free one
	for name, taken := range c.sessions {
		if !taken {
			c.sessions[name] = true
			// init the client for the session before passing it back
			svc, err := newSpanner(ctx)
			if err != nil {
				return nil, errors.Wrap(err, "unable to init spanner service")
			}
			return &session{name: name, sess: svc.Projects.Instances.Databases.Sessions},
				nil
		}
	}
	return nil, errors.Errorf("all %d sessions are in use. you may need to increase your session pool size.",
		len(c.sessions))
}

func (c *client) newSession(ctx context.Context) (*session, error) {
	svc, err := newSpanner(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to init spanner service")
	}
	sess := svc.Projects.Instances.Databases.Sessions
	resp, err := sess.Create(c.conn, &spanner.CreateSessionRequest{}).Do()
	if err != nil {
		return nil, errors.Wrap(err, "unable to init spanner session")
	}
	return &session{name: resp.Name, sess: sess}, nil
}

func (c *client) ReleaseSession(ctx context.Context, sess Session) {
	c.smu.Lock()
	defer c.smu.Unlock()
	c.sessions[sess.Name()] = false
}

func (c *client) Close(ctx context.Context) error {
	svc, err := newSpanner(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to init spanner service")
	}
	sess := svc.Projects.Instances.Databases.Sessions

	c.smu.Lock()
	defer c.smu.Unlock()

	for s := range c.sessions {
		_, err := sess.Delete(s).Do()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Commit(ctx context.Context, mutations []*spanner.Mutation, opts *spanner.TransactionOptions) (*spanner.CommitResponse, error) {
	return s.sess.Commit(s.name, &spanner.CommitRequest{
		Mutations: mutations, SingleUseTransaction: opts,
	}).Context(ctx).Do()
}

func (s *session) Name() string {
	return s.name
}

func (s *session) ExecuteSQL(ctx context.Context, params []*Param, sql, queryMode string) (*spanner.ResultSet, error) {
	var (
		pTypes = map[string]spanner.Type{}
		pVals  = map[string]interface{}{}
	)
	for _, p := range params {
		pTypes[p.Name] = spanner.Type{Code: p.Type}
		pVals[p.Name] = p.Value
	}
	pJSON, err := json.Marshal(pVals)
	if err != nil {
		return nil, errors.Wrap(err, "unable to encode query params")
	}
	res, err := s.sess.ExecuteSql(s.name, &spanner.ExecuteSqlRequest{
		ParamTypes: pTypes,
		Params:     pJSON,
		QueryMode:  queryMode,
		Sql:        sql,
	}).Context(ctx).Do()
	return res, errors.Wrap(err, "unable to execute query")
}

func newSpanner(ctx context.Context) (*spanner.Service, error) {
	var client *http.Client
	if appengine.IsDevAppServer() {
		var err error
		client, err = google.DefaultClient(ctx, spanner.SpannerDataScope)
		if err != nil {
			return nil, errors.Wrap(err, "unable to init default client")
		}
	} else {
		client = oauth2.NewClient(ctx, google.AppEngineTokenSource(ctx, spanner.SpannerDataScope))
	}
	return spanner.New(client)
}
