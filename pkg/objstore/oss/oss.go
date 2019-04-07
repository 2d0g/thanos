package oss

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/objstore"
	cos "github.com/mozillazg/go-cos"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

// DirDelim is the delimiter used to model a directory structure in an object store bucket.
const dirDelim = "/"

// Bucket implements the store.Bucket interface against cos-compatible(Tencent Object Storage) APIs.
type Bucket struct {
	logger log.Logger
	bkt    *oss.Bucket
	name   string
}

// Config encapsulates the necessary config values to instantiate an cos client.
type Config struct {
	Bucket    string `yaml:"bucket"`
	Endpoint  string `yaml:"endpoint"`
	AccessID  string `yaml:"access_id"`
	AccessKey string `yaml:"access_key"`
}

// Validate checks to see if mandatory cos config options are set.
func (conf *Config) validate() error {
	if conf.Bucket == "" ||
		conf.Endpoint == "" ||
		conf.AccessID == "" ||
		conf.AccessKey == "" {
		return errors.New("insufficient oss configuration information")
	}
	return nil
}

func NewBucket(logger log.Logger, conf []byte, component string) (*Bucket, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	var config Config
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return nil, errors.Wrap(err, "parsing oss configuration")
	}
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "validate oss configuration")
	}

	client, err := oss.New(config.Endpoint, config.AccessID, config.AccessKey)
	if err != nil {
		return nil, errors.Wrap(err, "initialize oss client")
	}
	bucket, err := client.Bucket(config.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "new bucket")
	}

	bkt := &Bucket{
		logger: logger,
		bkt:    bucket,
		name:   config.Bucket,
	}
	return bkt, nil
}

// Name returns the bucket name for COS.
func (b *Bucket) Name() string {
	return b.name
}

// Upload the contents of the reader as an object into the bucket.
func (b *Bucket) Upload(ctx context.Context, name string, r io.Reader) error {
	// create tmp file
	tmpFilename := "/tmp/thanos.tmp"
	fo, err := os.Create(tmpFilename)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	// make a write buffer
	w := bufio.NewWriter(fo)

	// make a buffer to keep chunks that are read
	buf := make([]byte, 1024*1024*1024)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			panic(err)
		}
	}

	if err = w.Flush(); err != nil {
		panic(err)
	}

	err = b.bkt.UploadFile(name, tmpFilename, 1024*1024*1024*2)
	if err != nil {
		return errors.Wrap(err, "upload oss object")
	}

	err = os.Remove(tmpFilename)
	if err != nil {
		return errors.Wrap(err, "deleted tmp uploadFile")
	}

	return nil
}

// Delete removes the object with the given name.
func (b *Bucket) Delete(ctx context.Context, name string) error {
	err := b.bkt.DeleteObject(name)
	if err != nil {
		return errors.Wrap(err, "delete oss object")
	}
	return nil
}

// Iter calls f for each entry in the given directory (not recursive.). The argument to f is the full
// object name including the prefix of the inspected directory.
func (b *Bucket) Iter(ctx context.Context, dir string, f func(string) error) error {
	if dir != "" {
		dir = strings.TrimSuffix(dir, dirDelim) + dirDelim
	}
	marker := oss.Marker("")
	for {
		lor, err := b.bkt.ListObjects(oss.MaxKeys(1000), marker, oss.Prefix(dir), oss.Delimiter("/"))
		if err != nil {
			return err
		}
		var listNames []string
		for _, object := range lor.Objects {
			listNames = append(listNames, object.Key)
		}

		for _, prefix := range lor.CommonPrefixes {
			listNames = append(listNames, prefix)
		}

		for _, name := range listNames {
			if err := f(name); err != nil {
				return err
			}
		}

		marker = oss.Marker(lor.NextMarker)
		if !lor.IsTruncated {
			break
		}
	}
	return nil
}

func (b *Bucket) getRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if len(name) == 0 {
		return nil, errors.Errorf("given object name should not empty")
	}
	options := []oss.Option{oss.Range(off, off+length-1)}

	if length != -1 {
		//if err := setRange(opts, off, off+length-1); err != nil {
		//	return nil, err
		//}
		options = []oss.Option{oss.Range(off, off+length-1)}
	}

	body, err := b.bkt.GetObject(name, options...)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// Get returns a reader for the given object name.
func (b *Bucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.getRange(ctx, name, 0, -1)
	//body, err := b.bkt.GetObject(name)
	//if err != nil {
	//	return nil, errors.Wrap(err, "get oss object")
	//}
	//return body, nil
}

// GetRange returns a new range reader for the given object name and range.
func (b *Bucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	return b.getRange(ctx, name, off, length)
}

// Exists checks if the given object exists in the bucket.
func (b *Bucket) Exists(ctx context.Context, name string) (bool, error) {
	return b.bkt.IsObjectExist(name)
}

// IsObjNotFoundErr returns true if error means that object is not found. Relevant to Get operations.
func (b *Bucket) IsObjNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "StatusCode=404")
}

func (b *Bucket) Close() error { return nil }

type objectInfo struct {
	key string
	err error
}

func setRange(opts *cos.ObjectGetOptions, start, end int64) error {
	if start == 0 && end < 0 {
		opts.Range = fmt.Sprintf("bytes=%d", end)
	} else if 0 < start && end == 0 {
		opts.Range = fmt.Sprintf("bytes=%d-", start)
	} else if 0 <= start && start <= end {
		opts.Range = fmt.Sprintf("bytes=%d-%d", start, end)
	} else {
		return errors.Errorf("Invalid range specified: start=%d end=%d", start, end)
	}
	return nil
}

func configFromEnv() Config {
	c := Config{
		Bucket:    os.Getenv("OSS_BUCKET"),
		Endpoint:  os.Getenv("OSS_ENDPOINT"),
		AccessID:  os.Getenv("OSS_ACCESSID"),
		AccessKey: os.Getenv("OSS_ACCESSKEY"),
	}

	return c
}

// NewTestBucket creates test bkt client that before returning creates temporary bucket.
// In a close function it empties and deletes the bucket.
func NewTestBucket(t testing.TB) (objstore.Bucket, func(), error) {
	c := configFromEnv()
	if err := validateForTest(c); err != nil {
		return nil, nil, err
	}

	if c.Bucket != "" {
		if os.Getenv("THANOS_ALLOW_EXISTING_BUCKET_USE") == "" {
			return nil, nil, errors.New("OSS_BUCKET is defined. Normally this tests will create temporary bucket " +
				"and delete it after test. Unset COS_BUCKET env variable to use default logic. If you really want to run " +
				"tests against provided (NOT USED!) bucket, set THANOS_ALLOW_EXISTING_BUCKET_USE=true. WARNING: That bucket " +
				"needs to be manually cleared. This means that it is only useful to run one test in a time. This is due " +
				"to safety (accidentally pointing prod bucket for test) as well as OSS not being fully strong consistent.")
		}

		bc, err := yaml.Marshal(c)
		if err != nil {
			return nil, nil, err
		}

		b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
		if err != nil {
			return nil, nil, err
		}

		if err := b.Iter(context.Background(), "", func(f string) error {
			return errors.Errorf("bucket %s is not empty", c.Bucket)
		}); err != nil {
			return nil, nil, errors.Wrapf(err, "oss check bucket %s", c.Bucket)
		}

		t.Log("WARNING. Reusing", c.Bucket, "OSS bucket for OSS tests. Manual cleanup afterwards is required")
		return b, func() {}, nil
	}

	src := rand.NewSource(time.Now().UnixNano())

	tmpBucketName := strings.Replace(fmt.Sprintf("test_%x", src.Int63()), "_", "-", -1)
	if len(tmpBucketName) >= 31 {
		tmpBucketName = tmpBucketName[:31]
	}
	c.Bucket = tmpBucketName

	bc, err := yaml.Marshal(c)
	if err != nil {
		return nil, nil, err
	}

	b, err := NewBucket(log.NewNopLogger(), bc, "thanos-e2e-test")
	if err != nil {
		return nil, nil, err
	}

	//if _, err := b.bkt.Bucket.Put(context.Background(), nil); err != nil {
	//	return nil, nil, err
	//}
	t.Log("created temporary OSS bucket for OSS tests with name", tmpBucketName)

	return b, func() {
		t.Logf("deleting bucket ...")
		//objstore.EmptyBucket(t, context.Background(), b)
		//if _, err := b.client.Bucket.Delete(context.Background()); err != nil {
		//	t.Logf("deleting bucket %s failed: %s", tmpBucketName, err)
		//}
	}, nil
}

func validateForTest(conf Config) error {
	if conf.Endpoint == "" ||
		conf.AccessID == "" ||
		conf.AccessKey == "" ||
		conf.Bucket == "" {
		return errors.New("insufficient oss configuration information")
	}
	return nil
}
