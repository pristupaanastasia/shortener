// Package s3 is a cleanup.Archive in an S3-compatible object store.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config configures the connection.
type Config struct {
	Endpoint  string // host:port
	Bucket    string
	Prefix    string // prepended to object names, e.g. "staging/"
	AccessKey string
	SecretKey string
	Secure    bool // HTTPS
}

// Archive writes objects to one bucket.
type Archive struct {
	client *minio.Client
	bucket string
	prefix string
}

// Open connects and creates the bucket if it does not exist.
func Open(ctx context.Context, cfg Config) (*Archive, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.Secure,
		BucketLookup: minio.BucketLookupPath, // works without DNS per bucket
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client for %s: %w", cfg.Endpoint, err)
	}
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create bucket %q: %w", cfg.Bucket, err)
		}
	}
	return &Archive{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// Put implements cleanup.Archive.
func (a *Archive) Put(ctx context.Context, name string, data []byte) error {
	name = a.prefix + name
	_, err := a.client.PutObject(ctx, a.bucket, name, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/x-ndjson"})
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", a.bucket, name, err)
	}
	return nil
}

// Get returns an object's contents.
func (a *Archive) Get(ctx context.Context, name string) ([]byte, error) {
	name = a.prefix + name
	obj, err := a.client.GetObject(ctx, a.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", a.bucket, name, err)
	}
	defer obj.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(obj); err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", a.bucket, name, err)
	}
	return buf.Bytes(), nil
}

// List returns the names of the objects under prefix, as Put and Get name
// them (without the archive's own prefix).
func (a *Archive) List(ctx context.Context, prefix string) ([]string, error) {
	var names []string
	opts := minio.ListObjectsOptions{Prefix: a.prefix + prefix, Recursive: true}
	for obj := range a.client.ListObjects(ctx, a.bucket, opts) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list %s/%s%s: %w", a.bucket, a.prefix, prefix, obj.Err)
		}
		names = append(names, strings.TrimPrefix(obj.Key, a.prefix))
	}
	return names, nil
}
