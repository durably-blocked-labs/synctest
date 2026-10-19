package worker

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/shubhaankar/synctest/evaluations/framework"
)

var evaluationJob = flag.String("evaluation-job", "", "evaluation job directory")
var evaluationResume = flag.Bool("evaluation-resume", false, "resume an interrupted evaluation job")

func TestEvaluationWorker(t *testing.T) {
	if *evaluationJob == "" {
		t.Skip("evaluation-job is required outside the worker binary")
	}
	runtime.GOMAXPROCS(4)
	registry, err := Registry()
	if err != nil {
		t.Fatal(err)
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	interrupted := make(chan struct{})
	var once sync.Once
	go func() {
		for range signals {
			stopped := false
			once.Do(func() {
				close(interrupted)
				stopped = true
			})
			if !stopped {
				os.Exit(130)
			}
		}
	}()

	_, err = framework.RunWorker(t, registry, framework.WorkerConfig{
		Directory: *evaluationJob, Resume: *evaluationResume, Interrupted: interrupted,
	})
	if err != nil {
		t.Fatal(err)
	}
}
