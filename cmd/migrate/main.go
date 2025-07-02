package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"ddb-to-firestore/internal/config"
	"ddb-to-firestore/internal/processor"
	"ddb-to-firestore/internal/utils"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	configFile string
	pairs      []string
	resume     bool
	dryRun     bool
	segments   int
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "ddb-to-firestore",
		Short: "DynamoDB to Firestore migration tool",
		Long:  "A tool for migrating data from DynamoDB to Firestore with support for one-time migration and live replication",
	}

	var migrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "Perform one-time migration from DynamoDB to Firestore",
		RunE:  runMigrate,
	}

	var liveCmd = &cobra.Command{
		Use:   "live",
		Short: "Start live replication from DynamoDB to Firestore using streams",
		RunE:  runLive,
	}

	var validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate configuration file",
		RunE:  runValidate,
	}

	var statusCmd = &cobra.Command{
		Use:   "status",
		Short: "Check migration status and progress",
		RunE:  runStatus,
	}

	var healthCmd = &cobra.Command{
		Use:   "health",
		Short: "Check health of connections and services",
		RunE:  runHealth,
	}

	// Add persistent flags
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "config.json", "Configuration file path")
	rootCmd.PersistentFlags().StringSliceVarP(&pairs, "pairs", "p", []string{}, "Specific database pairs to process (comma-separated)")
	rootCmd.PersistentFlags().BoolVar(&resume, "resume", false, "Resume from last checkpoint")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Validate configuration and show what would be migrated without actually doing it")

	// Add migrate-specific flags
	migrateCmd.Flags().IntVar(&segments, "segments", 0, "Number of parallel segments for DynamoDB scanning (0 = auto)")

	// Add commands
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(liveCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(healthCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runMigrate(cmd *cobra.Command, args []string) error {
	return runWithMode("migrate")
}

func runLive(cmd *cobra.Command, args []string) error {
	return runWithMode("live")
}

func runWithMode(mode string) error {
	// Load configuration
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Override mode if specified via command
	cfg.Migration.Mode = mode

	// Initialize logger
	logger, err := utils.NewLogger(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("Starting DynamoDB to Firestore migration",
		zap.String("mode", mode),
		zap.String("config", configFile),
		zap.Strings("pairs", pairs),
		zap.Bool("resume", resume),
		zap.Bool("dryRun", dryRun))

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel()
	}()

	// Create processor
	proc, err := processor.New(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}
	defer proc.Close()

	// Filter pairs if specified
	if len(pairs) > 0 {
		cfg.DatabasePairs = filterPairs(cfg.DatabasePairs, pairs)
		if len(cfg.DatabasePairs) == 0 {
			return fmt.Errorf("no matching database pairs found for: %v", pairs)
		}
	}

	// Override segments if specified
	if segments > 0 {
		cfg.Parallelism.SegmentCount = segments
	}

	// Run migration/replication
	switch mode {
	case "migrate":
		return proc.RunMigration(ctx, resume, dryRun)
	case "live":
		return proc.RunLiveReplication(ctx, resume, dryRun)
	default:
		return fmt.Errorf("invalid mode: %s", mode)
	}
}

func runValidate(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	fmt.Printf("Configuration is valid!\n")
	fmt.Printf("Mode: %s\n", cfg.Migration.Mode)
	fmt.Printf("Database pairs: %d\n", len(cfg.DatabasePairs))
	fmt.Printf("Firestore connection: %s\n", maskConnectionString(cfg.Firestore.ConnectionString))

	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	logger, err := utils.NewLogger(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	proc, err := processor.New(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}
	defer proc.Close()

	return proc.ShowStatus(context.Background(), pairs)
}

func runHealth(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	logger, err := utils.NewLogger(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	proc, err := processor.New(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create processor: %w", err)
	}
	defer proc.Close()

	return proc.HealthCheck(context.Background())
}

func filterPairs(allPairs []config.DatabasePair, selectedNames []string) []config.DatabasePair {
	nameSet := make(map[string]bool)
	for _, name := range selectedNames {
		nameSet[name] = true
	}

	var filtered []config.DatabasePair
	for _, pair := range allPairs {
		if nameSet[pair.Name] {
			filtered = append(filtered, pair)
		}
	}

	return filtered
}

func maskConnectionString(connStr string) string {
	if len(connStr) < 20 {
		return "***"
	}
	return connStr[:10] + "***" + connStr[len(connStr)-10:]
}
