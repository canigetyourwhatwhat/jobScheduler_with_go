package main

import (
	"context"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"time"
)

type Config struct {
	FailedTimeOutDuration time.Duration `yaml:"failedTimeOutDuration"`
	Jobs                  []JobData     `yaml:"jobs"`
}

type JobData struct {
	// name of the job
	Name string `yaml:"name"`
	// the length how long it should wait after completed the job
	WaitDuration time.Duration `yaml:"waitDuration"`
}

func main() {

	// Imagine we get some data from the configuration file
	file, cfgErr := os.Open("config.yml")
	if cfgErr != nil {
		panic("failed to read from the configuration file")
	}
	decoder := yaml.NewDecoder(file)
	var cfg *Config
	if err := decoder.Decode(&cfg); err != nil {
		panic("failed to decode from the configuration file")
	}

	for {
		if err := executeJobs(*cfg); err != nil {
			// here you can implement sentry or some other logging system
			log.Printf("%v happened! \n", err)
			// wait if we caught an error
			<-time.After(cfg.FailedTimeOutDuration)
			log.Print("service restarted! \n")
		}
	}
}

type Job struct {
	// it checks whether the job is ready to run or not
	// if empty struct is inside, it means it's ready
	Flag chan struct{}
	// the length how long it should wait after completed the job
	WaitTime time.Duration
}

type JobScheduler struct {
	availableJobMap map[string]Job
}

// NewJobScheduler takes configuration to set up the JobScheduler instance
func NewJobScheduler(cfg Config) JobScheduler {
	var jobScheduler JobScheduler
	jobScheduler.availableJobMap = make(map[string]Job)
	for _, jobData := range cfg.Jobs {
		jobScheduler.availableJobMap[jobData.Name] = jobScheduler.prepareJob(jobData)
	}
	return jobScheduler
}

func (js *JobScheduler) prepareJob(jobData JobData) Job {
	job := Job{
		Flag:     make(chan struct{}, 1),
		WaitTime: jobData.WaitDuration,
	}
	// To allow all the jobs to run at the beginning, it sends empty struct
	job.Flag <- struct{}{}
	return job
}

func executeJobs(cfg Config) error {
	// Receive error if happened
	errCha := make(chan error, 1)
	// It is used to cancel the process if other process returned an error
	ctx := context.Background()
	// Create an instance for job scheduler
	jobScheduler := NewJobScheduler(cfg)
	// Limit the number of jobs to be executed

	concurrentJobNum := 2
	// Fill the channel with the number of jobs that can run concurrently
	semaphore := make(chan struct{}, concurrentJobNum)
	for i := 0; i < concurrentJobNum; i++ {
		semaphore <- struct{}{}
	}

	for {
		select {
		case err := <-errCha:
			ctx.Done()
			return err
		case <-semaphore:
		default:
			select {
			case <-jobScheduler.availableJobMap["job1"].Flag:
				go func() {
					jobScheduler.executeJob(ctx, "job1", job1, errCha, semaphore)
				}()
			case <-jobScheduler.availableJobMap["job2"].Flag:
				go func() {
					jobScheduler.executeJob(ctx, "job2", job2, errCha, semaphore)
				}()
			case <-jobScheduler.availableJobMap["job3"].Flag:
				go func() {
					jobScheduler.executeJob(ctx, "job3", job3, errCha, semaphore)
				}()
			case <-jobScheduler.availableJobMap["job4"].Flag:
				go func() {
					jobScheduler.executeJob(ctx, "job4", job4, errCha, semaphore)
				}()
			}
		}
	}
}

func (js *JobScheduler) executeJob(
	ctx context.Context,
	jobName string,
	jobFunc func(ctx context.Context) error,
	errCha chan error,
	semaphore chan struct{},
) {
	log.Println(jobName + ": started running")
	if err := jobFunc(ctx); err != nil {
		errCha <- err
		return
	}
	//log.Println(jobName + ": finished, released semaphore")
	log.Println(jobName + ": finished execution")
	semaphore <- struct{}{}
	time.Sleep(js.availableJobMap[jobName].WaitTime)
	log.Println(jobName + ": ready")
	js.availableJobMap[jobName].Flag <- struct{}{}
}

func job1(ctx context.Context) error {
	time.Sleep(time.Second * 3)
	return checkContext(ctx)
}

func job2(ctx context.Context) error {
	time.Sleep(time.Second * 2)
	return checkContext(ctx)
}

func job3(ctx context.Context) error {
	time.Sleep(time.Second * 4)
	return checkContext(ctx)
}

func job4(ctx context.Context) error {
	//time.Sleep(time.Second * 5)
	//return nil
	return errors.New("----- error -----")
}

// if you want to actually see what happens if error happened during the process,
// you can use this function to see
func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
