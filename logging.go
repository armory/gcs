package gcs

import (
	"context"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"

	"time"

	"cloud.google.com/go/storage"
)

const (
	bufferSize = 1024
)

//this exists so we can stub out the dependency on creating gcs objects
type gcsWriteCreator struct {
	bkt *storage.BucketHandle
}

type writerCreator interface {
	createWriter(ctx context.Context, name string) io.WriteCloser
}

func (g *gcsWriteCreator) createWriter(ctx context.Context, name string) io.WriteCloser {
	wc := g.bkt.Object(name).NewWriter(ctx)

	wc.CacheControl = "private"
	wc.ACL = []storage.ACLRule{{
		Entity: storage.AllAuthenticatedUsers,
		Role:   storage.RoleReader,
	}}
	return wc
}

//TimeRotator stores relevant pointers to create and generator a writer
type TimeRotator struct {
	ctx            context.Context
	maxAge         time.Duration
	eventBuffer    chan []byte
	path           string
	wc             io.WriteCloser
	uuid           string
	logCreatedTime time.Time
	writerCreator  writerCreator
	fileFmt        string
}

//NewTimeRotator returns a newly intitiated Writer
func NewTimeRotator(ctx context.Context, bkt string, path string, fileFmt string, uuid string, maxAge time.Duration) *TimeRotator {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bw := &TimeRotator{
		eventBuffer:   make(chan []byte, bufferSize),
		uuid:          uuid,
		ctx:           ctx,
		path:          path,
		fileFmt:       fileFmt,
		writerCreator: &gcsWriteCreator{bkt: client.Bucket(bkt)},
	}
	bw.initLog()
	go bw.channelReader()
	return bw
}
func (bw *TimeRotator) initLog() {
	bw.logCreatedTime = time.Now()
	ts := bw.logCreatedTime.Round(15 * time.Minute).Format(bw.fileFmt)
	name := fmt.Sprintf(`%s/%s_%s.txt`, bw.path, ts, bw.uuid)
	log.Infof("creating new file with name: %s", name)
	bw.wc = bw.writerCreator.createWriter(bw.ctx, name)
}

//Write adds the message to a queue to be written later
func (bw *TimeRotator) Write(e []byte) (int, error) {
	bw.eventBuffer <- e
	return len(e), nil
}

func (bw *TimeRotator) channelReader() {
	for {
		select {
		case e := <-bw.eventBuffer:
			log.Debug(e)
			bw.appendEvent(e)
		case <-bw.ctx.Done():
			return
		}
	}
}

//Close active blob
func (bw *TimeRotator) Close() error {
	log.Info("Closing blob")
	err := bw.wc.Close()
	if err != nil {
		log.Fatalf("could not close blog, closing application: %v", err)
	}
	bw.wc = nil
	return err
}

func (bw *TimeRotator) appendEvent(b []byte) (int, error) {
	now := time.Now()

	if bw.logCreatedTime.Add(bw.maxAge).Before(now) {
		err := bw.wc.Close()
		if err != nil {
			log.Fatalf("error when closing GCS object: %v", err)
		}
		bw.initLog()
	}
	b = append(b, byte('\n'))
	bytesWritten, err := bw.wc.Write(b)
	if err != nil {
		log.Fatalf("error when writing bytes %v", err)
	}

	log.Debugf("successfully wrote %d bytes", bytesWritten)
	return bytesWritten, err
}
