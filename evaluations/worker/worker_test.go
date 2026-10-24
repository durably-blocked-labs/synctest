package worker

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/shubhaankar/synctest/evaluations/bubblegun"
)

var jobDirectory = flag.String("bubblegun-job", "", "Bubble Gun job directory")
var resume = flag.Bool("bubblegun-resume", false, "resume an interrupted Bubble Gun job")

func TestBubbleGunWorker(t *testing.T) {
	if *jobDirectory == "" {
		t.Skip("bubblegun-job is required outside the worker binary")
	}
	registry := Registry()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	_, err := bubblegun.RunWorker(t, registry, bubblegun.WorkerConfig{
		Directory:   *jobDirectory,
		Resume:      *resume,
		Interrupted: ctx.Done(),
		HardStop: func() {
			os.Exit(124)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}
