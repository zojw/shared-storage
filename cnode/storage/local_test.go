package storage_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/engula/shared-storage/cnode/storage"
	"github.com/stretchr/testify/assert"
)

func TestLocalStorage(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	root := filepath.Join(os.TempDir(), fmt.Sprintf("engula-test-%d", time.Now().UnixNano()))
	defer func() {
		os.RemoveAll(root)
	}()
	s, err := storage.NewLocal(ctx, root)
	assert.NoError(err)

	const testBucket1 = "testbucket1"
	assert.NoError(s.CreateBucket(ctx, testBucket1))
	buckets, err := s.ListBuckets(ctx)
	assert.NoError(err)
	assert.Contains(buckets, testBucket1)

	var w storage.ObjectWriter
	w, err = s.PutObject(ctx, testBucket1, "obj1")
	assert.NoError(err)
	assert.NoError(w.Write(ctx, []byte("abcdef")))
	assert.NoError(w.Finish(ctx))

	var d []byte
	d, err = s.ReadObject(ctx, testBucket1, "obj1", 1, 3)
	assert.NoError(err)
	result := string(d)
	assert.Equal("bcd", result)

	assert.NoError(s.DeleteBucket(ctx, testBucket1))
	buckets, err = s.ListBuckets(ctx)
	assert.NoError(err)
	assert.NotContains(buckets, testBucket1)
}
