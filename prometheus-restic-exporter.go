package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"slices"
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
	addr                      = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	resticBinary              = flag.String("restic-binary", "restic", "The command to run as the restic command.")
	repoName                  = flag.String("repo-name", "repo", "Name for the repo.")
	printCommandOutput        = flag.Bool("print-command-output", false, "Print the restic command's stdout and stderr after each run.")
	printCommandOutputOnError = flag.Bool("print-command-output-on-error", false, "Print the restic command's stdout and stderr when restic fails.")
	ignoreErrors              = flag.Bool("ignore-errors", false, "Ignore errors when refreshing metrics, continuing the exporter's execution")
	refreshInterval           = flag.Duration("refresh-interval", time.Minute, "Time between refreshing metrics")

	snapshotLabelNames = []string{"id", "short_id", "hostname", "repo"}
	metricNamespace    = "restic"

	resticExporterRefreshCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Subsystem: "exporter",
		Name:      "refresh_count",
		Help:      "",
	}, []string{"hostname", "repo", "status"})

	snapshotSubsystem = "snapshot"
	latestSnapshotSubsystem = "snapshot_latest"

	snapshotProgramVersion = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "program_version",
		Help:      "",
	}, append(snapshotLabelNames, "program_version"))

	snapshotTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: snapshotSubsystem,
		Name:      "time_seconds",
		Help:      "",
	}, snapshotLabelNames)

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

	snapshotProgramVersionLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "program_version",
		Help:      "",
	}, append(snapshotLabelNames, "program_version"))

	snapshotTimeLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "time_seconds",
		Help:      "",
	}, snapshotLabelNames)

	snapshotFilesNewLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "files_new",
		Help:      "",
	}, snapshotLabelNames)

	snapshotFilesChangedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "files_changed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotFilesUnmodifiedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "files_unmodified",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsNewLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "dirs_new",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsChangedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "dirs_changed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDirsUnmodifiedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "dirs_unmodified",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataBlobsLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "data_blobs",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTreeBlobsLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "tree_blobs",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataAddedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "data_added",
		Help:      "",
	}, snapshotLabelNames)

	snapshotDataAddedPackedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "data_added_packed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTotalFilesProcessedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "total_files_processed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotTotalBytesProcessedLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
		Name:      "total_bytes_processed",
		Help:      "",
	}, snapshotLabelNames)

	snapshotBackupDurationSecondsLatest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Subsystem: latestSnapshotSubsystem,
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

	snapshotTime.DeletePartialMatch(partialMatchLabels)

	snapshotProgramVersion.DeletePartialMatch(partialMatchLabels)

	snapshotFilesNewLatest.DeletePartialMatch(partialMatchLabels)
	snapshotFilesNewLatest.DeletePartialMatch(partialMatchLabels)
	snapshotFilesChangedLatest.DeletePartialMatch(partialMatchLabels)
	snapshotFilesUnmodifiedLatest.DeletePartialMatch(partialMatchLabels)

	snapshotDirsNewLatest.DeletePartialMatch(partialMatchLabels)
	snapshotDirsChangedLatest.DeletePartialMatch(partialMatchLabels)
	snapshotDirsUnmodifiedLatest.DeletePartialMatch(partialMatchLabels)

	snapshotDataBlobsLatest.DeletePartialMatch(partialMatchLabels)
	snapshotTreeBlobsLatest.DeletePartialMatch(partialMatchLabels)

	snapshotDataAddedLatest.DeletePartialMatch(partialMatchLabels)
	snapshotDataAddedPackedLatest.DeletePartialMatch(partialMatchLabels)

	snapshotTotalFilesProcessedLatest.DeletePartialMatch(partialMatchLabels)
	snapshotTotalBytesProcessedLatest.DeletePartialMatch(partialMatchLabels)

	snapshotBackupDurationSecondsLatest.DeletePartialMatch(partialMatchLabels)

	snapshotTimeLatest.DeletePartialMatch(partialMatchLabels)

	snapshotProgramVersionLatest.DeletePartialMatch(partialMatchLabels)
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

	snapshotTime.WithLabelValues(snapshotLabelValues...).Set(float64(s.Time.Unix()))

	snapshotProgramVersion.WithLabelValues(append(snapshotLabelValues, s.ProgramVersion)...).Set(1)
}

func setMetricsFromSnapshotLatest(s *Snapshot, repoName string) {
	snapshotLabelValues := []string{s.Id, s.ShortId, s.Hostname, repoName}

	snapshotFilesNewLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesNew))
	snapshotFilesChangedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesChanged))
	snapshotFilesUnmodifiedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.FilesUnmodified))

	snapshotDirsNewLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsNew))
	snapshotDirsChangedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsChanged))
	snapshotDirsUnmodifiedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DirsUnmodified))

	snapshotDataBlobsLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataBlobs))
	snapshotTreeBlobsLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TreeBlobs))

	snapshotDataAddedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataAdded))
	snapshotDataAddedPackedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.DataAddedPacked))

	snapshotTotalFilesProcessedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TotalFilesProcessed))
	snapshotTotalBytesProcessedLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Summary.TotalBytesProcessed))

	snapshotBackupDurationSecondsLatest.WithLabelValues(snapshotLabelValues...).Set(s.Summary.BackupDuration().Seconds())

	snapshotTimeLatest.WithLabelValues(snapshotLabelValues...).Set(float64(s.Time.Unix()))

	snapshotProgramVersionLatest.WithLabelValues(append(snapshotLabelValues, s.ProgramVersion)...).Set(1)
}

func refreshSnapshotsMetrics(ctx context.Context, resticBinary, repoName string, printCommandOutput, printCommandOutputOnError bool) error {
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

	printOutput := func() {
		fmt.Println("--- STDOUT ---")
		fmt.Println(string(stdout))
		fmt.Println("--- STDERR ---")
		fmt.Println(string(stderr))

	}

	if printCommandOutput {
		printOutput()
	}

	err = cmd.Wait()
	if err != nil {
		if !printCommandOutput && printCommandOutputOnError {
			printOutput()
		}
		return fmt.Errorf("failed waiting for command to finish: %w", err)
	}

	var snapshots []Snapshot

	if err := json.Unmarshal(stdout, &snapshots); err != nil {
		return fmt.Errorf("failed unmarshalling snapshot json: %w", err)
	}

	slices.SortFunc(snapshots, func(a, b Snapshot) int {
		return cmp.Compare(a.Time.Unix(), b.Time.Unix())
	})

	deleteSnapshotMetricsForRepo(repoName)

	for _, s := range snapshots {
		setMetricsFromSnapshot(&s, repoName)
	}
	if len(snapshots) > 0 {
		setMetricsFromSnapshotLatest(&snapshots[len(snapshots)-1], repoName)
	}

	return nil
}

func main() {
	flag.Parse()

	slog.Info("Running", "addr", *addr, "resticBinary", *resticBinary, "printCommandOutput", *printCommandOutput, "printCommandOutputOnError", *printCommandOutputOnError, "ignoreErrors", *ignoreErrors)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		defer stop()

		t := time.NewTicker(*refreshInterval)

		hostname, err := os.Hostname()
		if err != nil {
			slog.Error("failed to get hostname", "error", err)
			return
		}

		resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "error_ignored")
		resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "failed")
		resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "succeeded")

		for {
			slog.Info("refreshing snapshot metrics", "resticBinary", resticBinary, "repo", repoName)
			err := refreshSnapshotsMetrics(ctx, *resticBinary, *repoName, *printCommandOutput, *printCommandOutputOnError)
			if err != nil {
				if *ignoreErrors {
					slog.Info("failed to refresh snapshot metrics but ignoring errors", "error", err)
					resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "error_ignored").Inc()
				} else {
					slog.Error("failed to refresh snapshot metrics", "error", err)
					stop()
					resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "failed").Inc()
					return
				}
			} else {
				slog.Info("refreshed snapshot metrics", "resticBinary", resticBinary, "repo", repoName)
				resticExporterRefreshCount.WithLabelValues(hostname, *repoName, "succeeded").Inc()
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
