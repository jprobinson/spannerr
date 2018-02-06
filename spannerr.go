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
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	spanner "google.golang.org/api/spanner/v1"
	"google.golang.org/appengine"
)

type (
	// Client allows users to manage sessions on Google Cloud Spanner.
	Client struct {
		smu      sync.Mutex
		sessions map[string]*sessionInfo

		conn        string
		maxSessions int
	}

	// Session represents a live session on Google Cloud Spanner.
	Session struct {
		name string
		sess *spanner.ProjectsInstancesDatabasesSessionsService
	}

	// Param contains the information required to pass a parameter to a Cloud Spanner query.
	Param struct {
		// Name is the name of the parameter used within the sql.
		Name string
		// Value is the value of the parameter to pass into the query.
		Value interface{}
		// Type will be used to populate the spanner.Type.Code field. More details
		// can be found here: https://godoc.org/google.golang.org/api/spanner/v1#Type
		Type string
		// ArrayElementType will be used to populate the spanner.Type.Code field of a
		// nested array type. More details can be found here:
		// https://godoc.org/google.golang.org/api/spanner/v1#Type
		ArrayElementType string
	}

	sessionInfo struct {
		inUse    bool
		lastUsed time.Time
	}
)

// NewClient returns a new Client implementation.
func NewClient(project, instances, database string, maxSessions int) *Client {
	return &Client{
		conn:        "projects/" + project + "/instances/" + instances + "/databases/" + database,
		maxSessions: maxSessions,
		sessions:    map[string]*sessionInfo{},
	}
}

var idleTimeout = 45 * time.Minute

// AcquireSession will pull an existing session from the local cache. If the session
// cache is not full, it will create a new session and put it in the cache.
// Users must pass the Session to ReleaseSession when work is complete.
func (c *Client) AcquireSession(ctx context.Context) (*Session, error) {
	c.smu.Lock()
	defer c.smu.Unlock()
	// fill the buffer first
	if len(c.sessions) < c.maxSessions {
		sess, err := c.newSession(ctx)
		if err != nil {
			return nil, err
		}
		c.sessions[sess.name] = &sessionInfo{inUse: true}
		return sess, nil
	}
	// range over existing sessions until we find a free one
	for name, info := range c.sessions {
		if info.inUse {
			continue
		}
		// if session has been idle for too long, toss it out and make a new one
		if time.Now().UTC().Sub(info.lastUsed) > idleTimeout {
			delete(c.sessions, name)
			sess, err := c.newSession(ctx)
			if err != nil {
				return nil, err
			}
			c.sessions[sess.name] = &sessionInfo{inUse: true}
			return sess, nil
		}

		c.sessions[name] = &sessionInfo{inUse: true}
		// init the client for the session before passing it back
		svc, err := newSpanner(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "unable to init spanner service")
		}
		return &Session{name: name, sess: svc.Projects.Instances.Databases.Sessions},
			nil
	}
	return nil, errors.Errorf("all %d sessions are in use. you may need to increase your session pool size.",
		len(c.sessions))
}

func (c *Client) newSession(ctx context.Context) (*Session, error) {
	svc, err := newSpanner(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to init spanner service")
	}
	sess := svc.Projects.Instances.Databases.Sessions
	resp, err := sess.Create(c.conn, &spanner.CreateSessionRequest{}).Do()
	if err != nil {
		return nil, errors.Wrap(err, "unable to init spanner session")
	}
	return &Session{name: resp.Name, sess: sess}, nil
}

// ReleaseSession will make the session available in the cache again. Call this after
// first acquiring a session.
func (c *Client) ReleaseSession(ctx context.Context, sess Session) {
	c.smu.Lock()
	defer c.smu.Unlock()
	c.sessions[sess.name] = &sessionInfo{inUse: false, lastUsed: time.Now().UTC()}
}

// Close will attempt to end all existing sessions. If you have shutdown hooks
// available for your instance type, call this then.
// If you do not have shutdown hooks, the sessions made will be closed automatically
// after one hour of idle time: https://cloud.google.com/spanner/docs/sessions
func (c *Client) Close(ctx context.Context) error {
	svc, err := newSpanner(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to init spanner service")
	}
	sess := svc.Projects.Instances.Databases.Sessions

	c.smu.Lock()
	defer c.smu.Unlock()

	for s := range c.sessions {
		_, err := sess.Delete(s).Context(ctx).Do()
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit commits a transaction. The request includes the mutations to be applied to
// rows in the database.
// This function wraps https://godoc.org/google.golang.org/api/spanner/v1#ProjectsInstancesDatabasesSessionsService.Commit
func (s *Session) Commit(ctx context.Context, mutations []*spanner.Mutation, opts *spanner.TransactionOptions) (*spanner.CommitResponse, error) {
	return s.sess.Commit(s.name, &spanner.CommitRequest{
		Mutations: mutations, SingleUseTransaction: opts,
	}).Context(ctx).Do()
}

// ExecuteSQL executes an SQL query, returning all rows in a single reply.
// This function wraps https://godoc.org/google.golang.org/api/spanner/v1#ProjectsInstancesDatabasesSessionsExecuteSqlCall
func (s *Session) ExecuteSQL(ctx context.Context, params []*Param, sql, queryMode string) (*spanner.ResultSet, error) {
	var (
		pTypes = map[string]spanner.Type{}
		pVals  = map[string]interface{}{}
	)
	for _, p := range params {
		var aryType *spanner.Type
		if p.ArrayElementType != "" {
			aryType = &spanner.Type{Code: p.ArrayElementType}
		}
		pTypes[p.Name] = spanner.Type{Code: p.Type, ArrayElementType: aryType}
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
