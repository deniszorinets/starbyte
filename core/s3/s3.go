package s3io

import (
	"context"
	"io"

	"regexp"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Client struct {
	client *minio.Client
}

var UriRegexp = regexp.MustCompile(`(?P<Protocol>http|https)://(?P<AccessKey>.+):(?P<SecretKey>.+)@(?P<Host>[\w+\.]+)(?P<Port>:\d+)?/(?P<Bucket>\w+)/(?P<Object>.*)`)

type s3ClientConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

func newS3Client(config s3ClientConfig) (*s3Client, error) {
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.UseSSL,
	})

	if err != nil {
		return nil, err
	}

	return &s3Client{client: minioClient}, nil
}

func parseFileUri(uri string) (s3ClientConfig, string, string) {
	matches := UriRegexp.FindStringSubmatch(uri)
	groups := UriRegexp.SubexpNames()

	s3Conf := s3ClientConfig{}

	host := ""
	bucket := ""
	object := ""

	for i := 1; i < len(matches); i++ {
		switch groups[i] {
		case "Protocol":
			if matches[i] == "https" {
				s3Conf.UseSSL = true
			}
		case "AccessKey":
			s3Conf.AccessKey = matches[i]
		case "SecretKey":
			s3Conf.SecretKey = matches[i]
		case "Host":
			host += matches[i]
		case "Port":
			host += matches[i]
		case "Bucket":
			bucket = matches[i]
		case "Object":
			object = matches[i]
		}
	}

	s3Conf.Endpoint = host
	return s3Conf, bucket, object
}

func Read(ctx context.Context, uri string) (io.ReadCloser, error) {
	s3Conf, bucket, object := parseFileUri(uri)
	client, err := newS3Client(s3Conf)

	if err != nil {
		return nil, err
	}

	obj, err := client.client.GetObject(ctx, bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, err
}

func Write(ctx context.Context, uri string, reader io.ReadCloser) error {
	s3Conf, bucket, object := parseFileUri(uri)
	client, err := newS3Client(s3Conf)

	if err != nil {
		return err
	}
	_, err = client.client.PutObject(ctx, bucket, object, reader, -1, minio.PutObjectOptions{})
	return err
}
