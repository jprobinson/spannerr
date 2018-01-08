# Spanner R is a convenience wrapper for Google Cloud Spanner's REST client

[Cloud Spanner' REST API docs](https://godoc.org/google.golang.org/api/spanner/v1)

The main purpose of this client is to add a layer of session management. More inforomation on Spanner sessions can be found here: https://cloud.google.com/spanner/docs/sessions

If you are not on running your services on App Engine, you should just use the [official Spanner (gRPC) client](https://godoc.org/cloud.google.com/go/spanner)
