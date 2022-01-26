package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	_ Storage = &s3Storage{}
)

const (
	objectCategory = "buckets"
	metaPrefix     = "engula-meta"
)

var (
	ErrInvalidBucketName = errors.New("invalid bucket name")
)

type s3Storage struct {
	tenant   string
	category string

	s3Bucket *string
	client   *s3.S3
}

func (s *s3Storage) Init(ctx context.Context) (err error) {
	return
}

func (s *s3Storage) Destory(ctx context.Context) (err error) {
	return
}

func New(ctx context.Context, tenant, s3Bucket, region string) (s Storage, err error) {
	var ss *session.Session
	if ss, err = session.NewSession(); err != nil {
		return
	}
	client := s3.New(ss, &aws.Config{Region: &region})

	s3 := &s3Storage{tenant: tenant, s3Bucket: &s3Bucket, category: objectCategory, client: client}
	if err = s3.createS3BucketIfNotExist(ctx); err != nil {
		return
	}
	s = s3

	return
}

func (s *s3Storage) createS3BucketIfNotExist(ctx context.Context) (err error) {
	if _, err = s.client.HeadBucketWithContext(ctx, &s3.HeadBucketInput{Bucket: s.s3Bucket}); err == nil {
		return
	}
	if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != "NotFound" {
		return
	}

	_, err = s.client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{Bucket: s.s3Bucket})
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "BucketAlreadyOwnedByYou" {
		err = nil
		return
	}
	return err
}

func (s *s3Storage) metaRoot() *string {
	path := fmt.Sprintf("%s/%s/%s", s.tenant, s.category, metaPrefix)
	return &path
}

func (s *s3Storage) metaKey(bucket string) *string {
	path := fmt.Sprintf("%s/%s", *s.metaRoot(), bucket)
	return &path
}

func (s *s3Storage) objectRoot(bucket string) *string {
	path := fmt.Sprintf("%s/%s/%s", s.tenant, s.category, bucket)
	return &path
}

func (s *s3Storage) objectKey(bucket, object string) *string {
	path := fmt.Sprintf("%s/%s", *s.objectRoot(bucket), object)
	return &path
}

func (s *s3Storage) CreateBucket(ctx context.Context, bucket string) (err error) {
	if bucket == metaPrefix {
		err = ErrInvalidBucketName
		return
	}
	_, err = s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{Bucket: s.s3Bucket, Key: s.metaKey(bucket)})
	return
}

func (s *s3Storage) DeleteBucket(ctx context.Context, bucket string) (err error) {
	_, err = s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{Bucket: s.s3Bucket, Key: s.metaKey(bucket)})
	return
}

func (s *s3Storage) ListBuckets(ctx context.Context) (buckets []string, err error) {
	var output *s3.ListObjectsV2Output
	if output, err = s.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{Bucket: s.s3Bucket, Prefix: s.metaRoot()}); err != nil {
		return
	}
	prefix := *s.metaRoot() + "/"
	buckets = make([]string, 0, len(output.Contents))
	for _, c := range output.Contents {
		bucket := strings.TrimPrefix(*c.Key, prefix)
		buckets = append(buckets, bucket)
	}
	return
}

func (s *s3Storage) DeleteObject(ctx context.Context, bucket, object string) (err error) {
	_, err = s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{Bucket: s.s3Bucket, Key: s.objectKey(bucket, object)})
	return
}

func (s *s3Storage) ListObjects(ctx context.Context, bucket string) (objects []string, err error) {
	var output *s3.ListObjectsV2Output
	if output, err = s.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{Bucket: s.s3Bucket, Prefix: s.objectRoot(bucket)}); err != nil {
		return
	}
	prefix := *s.objectRoot(bucket) + "/"
	objects = make([]string, 0, len(output.Contents))
	for _, c := range output.Contents {
		object := strings.TrimPrefix(*c.Key, prefix)
		objects = append(objects, object)
	}
	return
}

func (s *s3Storage) ReadObject(ctx context.Context, bucket, object string, pos, len int32) (bytes []byte, err error) {
	var (
		output   *s3.GetObjectOutput
		getRange = fmt.Sprintf("bytes=%d-%d", pos, pos+len-1)
	)
	if output, err = s.client.GetObjectWithContext(ctx, &s3.GetObjectInput{Bucket: s.s3Bucket, Key: s.objectKey(bucket, object), Range: &getRange}); err != nil {
		return
	}
	bytes, err = io.ReadAll(output.Body)
	return
}

func (s *s3Storage) PutObject(ctx context.Context, bucket, object string) (writer ObjectWriter, err error) {
	var output *s3.CreateMultipartUploadOutput
	key := s.objectKey(bucket, object)
	if output, err = s.client.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{Bucket: s.s3Bucket, Key: key}); err != nil {
		return
	}

	writer = &s3Uploader{
		storage:   s,
		uploadId:  output.UploadId,
		uploadKey: key,
	}
	return
}

const uploadPartSize = 8 * 1024 * 1024

var (
	_ ObjectWriter = &s3Uploader{}
)

type s3Uploader struct {
	uploadId  *string
	uploadKey *string
	buf       []byte

	storage *s3Storage
}

func (u *s3Uploader) Write(ctx context.Context, p []byte) (err error) {
	u.buf = append(u.buf, p...)
	return
}

func (u *s3Uploader) Finish(ctx context.Context) (err error) {
	var (
		partNum   = int64(1)
		completes []*s3.CompletedPart
	)
	if len(u.buf) < uploadPartSize*2 {
		var c *s3.CompletedPart
		if c, err = u.uploadPart(ctx, partNum, u.buf); err != nil {
			return
		}
		completes = append(completes, c)
		partNum++
	} else {
		for len(u.buf) >= uploadPartSize*2 {
			var c *s3.CompletedPart
			if c, err = u.uploadPart(ctx, partNum, u.buf[:uploadPartSize]); err != nil {
				return
			}
			completes = append(completes, c)
			partNum++
			u.buf = u.buf[uploadPartSize:]
		}
		var c *s3.CompletedPart
		if c, err = u.uploadPart(ctx, partNum, u.buf); err != nil {
			return
		}
		completes = append(completes, c)
	}
	u.buf = nil

	_, err = u.storage.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: u.storage.s3Bucket, Key: u.uploadKey, UploadId: u.uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completes}})
	return
}

func (u *s3Uploader) uploadPart(ctx context.Context, partNum int64, partData []byte) (complete *s3.CompletedPart, err error) {
	var output *s3.UploadPartOutput
	if output, err = u.storage.client.UploadPartWithContext(ctx, &s3.UploadPartInput{Bucket: u.storage.s3Bucket,
		Key: u.uploadKey, PartNumber: &partNum, UploadId: u.uploadId, Body: bytes.NewReader(partData)}); err != nil {
		return
	}
	complete = &s3.CompletedPart{ETag: output.ETag, PartNumber: &partNum}
	return
}
