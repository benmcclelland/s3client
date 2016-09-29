package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/benmcclelland/tarstream"
	"github.com/facebookgo/flagconfig"
)

type config struct {
	awsID           string
	awsSecret       string
	awsRegion       string
	bucket          string
	endpoint        string
	filePath        string
	objectPath      string
	operation       string
	filelist        string
	checksumDisable bool
	disableSSL      bool
	pathStyle       bool
	partSize        int64
	concurrency     int
	maxprocs        int
	debug           bool
}

func (c *config) getCreds() *credentials.Credentials {
	// TODO support token/IAM
	if c.awsID == "" {
		c.awsID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if c.awsID == "" {
		log.Fatal("no AWS_ACCESS_KEY_ID found")
	}
	if c.awsSecret == "" {
		c.awsSecret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if c.awsSecret == "" {
		log.Fatal("no AWS_SECRET_ACCESS_KEY found")
	}

	return credentials.NewStaticCredentials(c.awsID, c.awsSecret, "")
}

func (c *config) getConfig(creds *credentials.Credentials) *aws.Config {
	config := aws.NewConfig().WithRegion(c.awsRegion).WithCredentials(creds)
	config = config.WithDisableSSL(c.disableSSL)
	config = config.WithDisableComputeChecksums(c.checksumDisable)
	config = config.WithS3ForcePathStyle(c.pathStyle)
	if c.debug == true {
		config = config.WithLogLevel(aws.LogDebugWithSigning)
	}
	if c.endpoint != "" {
		config = config.WithEndpoint(c.endpoint)
	}

	return config
}

func uploadFile(c *config) {
	var size int64
	var upinfo *s3manager.UploadInput

	if c.filelist == "" {
		fi, err := os.Lstat(c.filePath)
		if err != nil {
			log.Fatal(err)
		}
		size = fi.Size()
		file, err := os.Open(c.filePath)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		upinfo = &s3manager.UploadInput{
			Body:   file,
			Bucket: &c.bucket,
			Key:    &c.objectPath,
		}
	} else {
		tarfile, info, err := tarstream.GenVec(strings.Split(c.filelist, ","))
		if err != nil {
			log.Fatal(err)
		}
		size = tarfile.GetSize()
		upinfo = &s3manager.UploadInput{
			Body:   &tarfile,
			Bucket: &c.bucket,
			Key:    &c.objectPath,
		}

		fmt.Println("### TAR INFO ###")
		for _, i := range info {
			fmt.Println(" File:", i.Name, "Offset:", i.Offset)
		}
		fmt.Println("################")
	}

	creds := c.getCreds()
	config := c.getConfig(creds)

	uploader := s3manager.NewUploader(session.New(config))
	uploader.PartSize = c.partSize
	uploader.Concurrency = c.concurrency

	start := time.Now()

	result, err := uploader.Upload(upinfo)
	elapsed := time.Since(start)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Uploaded:", result.Location, float64(size/1048576)/elapsed.Seconds(), "MB/s")
}

func downloadFile(c *config) {
	file, err := os.Create(c.filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	creds := c.getCreds()
	config := c.getConfig(creds)

	downloader := s3manager.NewDownloader(session.New(config))
	downloader.PartSize = c.partSize
	downloader.Concurrency = c.concurrency

	start := time.Now()

	// TODO GetObjectInput can to range get
	size, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &c.objectPath,
	})

	elapsed := time.Since(start)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Downloaded:", c.filePath, float64(size/1048576)/elapsed.Seconds(), "MB/s")
}

func main() {
	c := &config{}
	flag.StringVar(&c.awsID, "id", "", "AWS access key or use env AWS_ACCESS_KEY_ID")
	flag.StringVar(&c.awsSecret, "secret", "", "AWS secret key or use env AWS_SECRET_ACCESS_KEY")
	flag.StringVar(&c.operation, "op", "upload", "operation to do upload/download")
	flag.StringVar(&c.filePath, "filepath", "", "path for local file to read/write")
	flag.StringVar(&c.objectPath, "object", "", "path for object read/write")
	flag.StringVar(&c.bucket, "bucket", "", "bucket for target operation")
	flag.StringVar(&c.endpoint, "endpoint", "", "endpoint if different than s3.amazonaws.com in the form of host:port")
	flag.StringVar(&c.awsRegion, "region", "us-west-2", "AWS region")
	flag.StringVar(&c.filelist, "filelist", "", "comma sep file list, if defined will upload tarball")
	flag.BoolVar(&c.checksumDisable, "nocsum", false, "disable checksum for uploads")
	flag.BoolVar(&c.disableSSL, "nossl", false, "disable https")
	flag.BoolVar(&c.pathStyle, "pathstyle", false, "force path style requests")
	flag.Int64Var(&c.partSize, "partsize", 64*1024*1024, "part size for uploads")
	flag.IntVar(&c.concurrency, "concurrency", 24, "upload concurrency for multipart uploads and downloads")
	flag.IntVar(&c.maxprocs, "maxprocs", 0, "GOMAXPROCS")
	flag.BoolVar(&c.debug, "debug", false, "enable debug output")
	flag.Parse()
	flagconfig.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if c.maxprocs > 0 {
		runtime.GOMAXPROCS(c.maxprocs)
	}

	switch c.operation {
	case "upload":
		uploadFile(c)
	case "download":
		downloadFile(c)
	default:
		log.Fatal("operation must be one of upload or download")
	}
}
