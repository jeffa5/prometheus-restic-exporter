package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Snapshot is the state of a resource at one point in time.
type Snapshot struct {
	Id       string    `json:"id"`
	ShortId  string    `json:"short_id"`
	Time     time.Time `json:"time"`
	Parent   *string   `json:"parent,omitempty"`
	Tree     *string   `json:"tree"`
	Paths    []string  `json:"paths"`
	Hostname string    `json:"hostname,omitempty"`
	Username string    `json:"username,omitempty"`
	UID      uint32    `json:"uid,omitempty"`
	GID      uint32    `json:"gid,omitempty"`
	Excludes []string  `json:"excludes,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Original *string   `json:"original,omitempty"`

	ProgramVersion string           `json:"program_version,omitempty"`
	Summary        *SnapshotSummary `json:"summary,omitempty"`
}

type SnapshotSummary struct {
	BackupStart time.Time `json:"backup_start"`
	BackupEnd   time.Time `json:"backup_end"`

	// statistics from the backup json output
	FilesNew            uint   `json:"files_new"`
	FilesChanged        uint   `json:"files_changed"`
	FilesUnmodified     uint   `json:"files_unmodified"`
	DirsNew             uint   `json:"dirs_new"`
	DirsChanged         uint   `json:"dirs_changed"`
	DirsUnmodified      uint   `json:"dirs_unmodified"`
	DataBlobs           int    `json:"data_blobs"`
	TreeBlobs           int    `json:"tree_blobs"`
	DataAdded           uint64 `json:"data_added"`
	DataAddedPacked     uint64 `json:"data_added_packed"`
	TotalFilesProcessed uint   `json:"total_files_processed"`
	TotalBytesProcessed uint64 `json:"total_bytes_processed"`
}

func (ss *SnapshotSummary) BackupDuration() time.Duration {
	return ss.BackupEnd.Sub(ss.BackupStart)
}

var (
	addr                   = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	resticBinary           = flag.String("restic-binary", "restic", "The command to run as the restic command, supply comma separated list for multiple repos.")
	printCommandOutput     = flag.Bool("print-command-output", false, "Print the restic command's stdout and stderr after each run.")
	ignoreResticErrorCodes = flag.String("ignore-restic-error-codes", "", "Error codes from restic that should be ignored, continuing the exporter's execution")
	refreshInterval        = flag.Duration("refresh-interval", time.Minute, "Time between refreshing metrics")

	snapshotLabelNames = []string{"id", "short_id", "hostname", "repo"}
	metricNamespace    = "restic"
	snapshotSubsystem  = "snapshot"

	snapshotProgramVersion = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "program_version",
		Help:      "",
	}, append(snapshotLabelNames, "program_version"))

	snapshotFilesNew = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "files_new",
		Help:      "",
	}, snapshotLabelNames)

	snapshotFilesChanged = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "files_changed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotFilesUnmodified = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "files_unmodified",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsNew = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "dirs_new",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsChanged = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "dirs_changed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsUnmodified = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "dirs_unmodified",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataBlobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "data_blobs",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTreeBlobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "tree_blobs",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataAdded = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "data_added",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataAddedPacked = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "data_added_packed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTotalFilesProcessed = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "total_files_processed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTotalBytesProcessed = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "total_bytes_processed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotBackupDurationSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "backup_duration_seconds",
		Help:      "",
	}, snapshotLabelNames)
)

// deleteSnapshotMetricsForRepo calls DeletePartialMatch to clear results from old pruned snapshots
func deleteSnapshotMetricsForRepo(repo string) {
	partialMatchLabels := prometheus.Labels{"repo": repo}
	snapshotFilesNew.DeletePartialMatch(partialMatchLabels)
	snapshotFilesNew.DeletePartialMatch(partialMatchLabels)
	snapshotFilesChanged.DeletePartialMatch(partialMatchLabels)
	snapshotFilesUnmodified.DeletePartialMatch(partialMatchLabels)

	snapshotDirsNew.DeletePartialMatch(partialMatchLabels)
	snapshotDirsChanged.DeletePartialMatch(partialMatchLabels)
	snapshotDirsUnmodified.DeletePartialMatch(partialMatchLabels)

	snapshotDataBlobs.DeletePartialMatch(partialMatchLabels)
	snapshotTreeBlobs.DeletePartialMatch(partialMatchLabels)

	snapshotDataAdded.DeletePartialMatch(partialMatchLabels)
	snapshotDataAddedPacked.DeletePartialMatch(partialMatchLabels)

	snapshotTotalFilesProcessed.DeletePartialMatch(partialMatchLabels)
	snapshotTotalBytesProcessed.DeletePartialMatch(partialMatchLabels)

	snapshotBackupDurationSeconds.DeletePartialMatch(partialMatchLabels)

	snapshotProgramVersion.DeletePartialMatch(partialMatchLabels)
}

func setMetricsFromSnapshot(s *Snapshot, repoName string) {
	snapshotLabelValues := []string{s.Id, s.ShortId, s.Hostname, repoName}

	snapshotFilesNew.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesNew))
	snapshotFilesChanged.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesChanged))
	snapshotFilesUnmodified.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesUnmodified))

	snapshotDirsNew.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsNew))
	snapshotDirsChanged.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsChanged))
	snapshotDirsUnmodified.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsUnmodified))

	snapshotDataBlobs.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataBlobs))
	snapshotTreeBlobs.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TreeBlobs))

	snapshotDataAdded.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataAdded))
	snapshotDataAddedPacked.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataAddedPacked))

	snapshotTotalFilesProcessed.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TotalFilesProcessed))
	snapshotTotalBytesProcessed.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TotalBytesProcessed))

	snapshotBackupDurationSeconds.WithLabelValues(snapshotLabelValues...).Set(s.Summary.BackupDuration().Seconds())

	snapshotProgramVersion.WithLabelValues(append(snapshotLabelValues, s.ProgramVersion)...).Set(1)
}

func refreshSnapshotsMetricsMultiple(ctx context.Context, resticBinaries []string, ignoreResticErrorCodes []int, printCommandOutput bool) error {
	for _, resticBinary := range resticBinaries {
		repoNameParts := strings.SplitN(resticBinary, "-", 2)
		repoName := repoNameParts[0]
		if len(repoName) > 1 {
			repoName = repoNameParts[1]
		}

		if err := refreshSnapshotsMetrics(ctx, resticBinary, repoName, ignoreResticErrorCodes, printCommandOutput); err != nil {
			return fmt.Errorf("failed refreshing snapshot metrics for repo %q: %w", repoName, err)
		}
	}
	return nil
}

func refreshSnapshotsMetrics(ctx context.Context, resticBinary, repoName string, ignoreResticErrorCodes []int, printCommandOutput bool) error {
	slog.Info("refreshing snapshot metrics", "resticBinary", resticBinary, "repo", repoName)

	cmd := exec.CommandContext(ctx, resticBinary, "snapshots", "--json")
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed getting stdout pipe on command: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed getting stderr pipe on command: %w", err)
	}

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed starting command: %w", err)
	}

	stdout, err := io.ReadAll(stdoutPipe)
	if err != nil {
		return fmt.Errorf("failed reading stdout from command: %w", err)
	}

	stderr, err := io.ReadAll(stderrPipe)
	if err != nil {
		return fmt.Errorf("failed reading stderr from command: %w", err)
	}

	if printCommandOutput {
		fmt.Println("--- STDOUT ---")
		fmt.Println(string(stdout))
		fmt.Println("--- STDERR ---")
		fmt.Println(string(stderr))
	}

	err = cmd.Wait()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		for _, code := range ignoreResticErrorCodes {
			if code == exitErr.ExitCode() {
				slog.Info("failed waiting for command to finish but exit code ignored", "code", code)
				return nil
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed waiting for command to finish (run with --print-command-output for the output): %w", err)
	}

	var snapshots []Snapshot

	if err := json.Unmarshal(stdout, &snapshots); err != nil {
		return fmt.Errorf("failed unmarshalling snapshot json: %w", err)
	}

	deleteSnapshotMetricsForRepo(repoName)

	for _, s := range snapshots {
		setMetricsFromSnapshot(&s, repoName)
	}

	return nil
}

func main() {
	flag.Parse()

	slog.Info("Running", "addr", *addr, "resticBinary", *resticBinary, "printCommandOutput", *printCommandOutput, "ignoreResticErrorCodes", *ignoreResticErrorCodes)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		defer stop()

		t := time.NewTicker(*refreshInterval)
		resticBinaries := strings.Split(*resticBinary, ",")

		ignoreResticErrorCodesStr := strings.Split(*ignoreResticErrorCodes, ",")
		var ignoreResticErrorCodes []int
		for _, code := range ignoreResticErrorCodesStr {
			codenum, err := strconv.Atoi(code)
			if err != nil {
				slog.Error("failed parsing ignore restic error code", "code", code)
			}
			ignoreResticErrorCodes = append(ignoreResticErrorCodes, codenum)
		}

		for {
			err := refreshSnapshotsMetricsMultiple(ctx, resticBinaries, ignoreResticErrorCodes, *printCommandOutput)
			if err != nil {
				slog.Error("failed to refresh snapshot metrics", "error", err)
				stop()
				return
			}

			select {
			case <-t.C:
			case <-ctx.Done():
				return
			}
		}
	})

	slog.Info("serving metrics", "address", *addr)
	server := http.Server{
		Addr: *addr,
	}
	http.Handle("/metrics", promhttp.Handler())
	wg.Go(func() {
		defer stop()
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("failed listen and serve", "error", err)
		} else {
			slog.Info("finished listen and serve")
		}
	})

	<-ctx.Done()
	server.Shutdown(ctx)

	wg.Wait()
}
