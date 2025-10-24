package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"os/signal"
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
	addr         = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	resticBinary = flag.String("restic-binary", "restic", "The command to run as the restic command.")
	printCommandOutput = flag.Bool("print-command-output", false, "Print the restic command's stdout and stderr after each run.")

	snapshotLabelNames = []string{"id", "short_id", "hostname"}
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

func setMetricsFromSnapshot(s *Snapshot) {
	snapshotLabelValues := []string{s.Id, s.ShortId, s.Hostname}

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

func refreshSnapshotsMetrics(ctx context.Context, resticBinary string, printCommandOutput bool) error {
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

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("failed waiting for command to finish (run with --print-command-output for the output): %w", err)
	}

	var snapshots []Snapshot

	if err := json.Unmarshal(stdout, &snapshots); err != nil {
		return fmt.Errorf("failed unmarshalling snapshot json: %w", err)
	}

	for _, s := range snapshots {
		setMetricsFromSnapshot(&s)
	}

	return nil
}

func main() {
	flag.Parse()

	slog.Info("Running", "addr", *addr, "resticBinary", *resticBinary)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		t := time.NewTicker(time.Minute)
		for {
			slog.Info("refreshing snapshot metrics")
			err := refreshSnapshotsMetrics(ctx, *resticBinary, *printCommandOutput)
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
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("failed listen and serve", "error", err)
		} else {
			slog.Info("finished listen and serve")
		}
		stop()
	})

	<-ctx.Done()
	server.Shutdown(ctx)

	wg.Wait()
}
