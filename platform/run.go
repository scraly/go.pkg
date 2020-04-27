/*
 * Copyright (C) Continental Automotive GmbH 2019
 * Alle Rechte vorbehalten. All Rights Reserved.
 * The reproduction, transmission or use of this document or its contents is not
 * permitted without express written authority. Offenders will be liable for
 * damages. All rights, including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 */

package platform

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
	"github.com/dchest/uniuri"
	"github.com/oklog/run"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/scraly/go.pkg/log"
	"github.com/scraly/go.pkg/platform/diagnostic"
	"github.com/scraly/go.pkg/platform/jaeger"
	"github.com/scraly/go.pkg/platform/ocagent"
	"github.com/scraly/go.pkg/platform/prometheus"
	"github.com/scraly/go.pkg/platform/runtime"
)

// -----------------------------------------------------------------------------

// Application represents platform application
type Application struct {
	Debug           bool
	Name            string
	Version         string
	Revision        string
	Instrumentation InstrumentationConfig
	Builder         func(upg *tableflip.Upgrader, group *run.Group)
}

// Run the dispatcher
func Run(ctx context.Context, app *Application) error {

	// Generate an instance identifier
	appID := uniuri.NewLen(32)

	// Prepare logger
	log.Setup(ctx, &log.Options{
		Debug:     app.Debug,
		AppName:   app.Name,
		AppID:     appID,
		Version:   app.Version,
		Revision:  app.Revision,
		SentryDSN: app.Instrumentation.Logs.SentryDSN,
		LogLevel:  app.Instrumentation.Logs.Level,
	})

	// Preparing instrumentation
	instrumentationRouter := http.NewServeMux()

	// Register common features
	if app.Instrumentation.Diagnostic.Enabled {
		cancelFunc, err := diagnostic.Register(ctx, app.Instrumentation.Diagnostic.Config, instrumentationRouter)
		if err != nil {
			log.For(ctx).Fatal("Unable to register diagnostic instrumentation", zap.Error(err))
		}
		defer cancelFunc()
	}
	if app.Instrumentation.Prometheus.Enabled {
		if _, err := prometheus.RegisterExporter(ctx, app.Instrumentation.Prometheus.Config, instrumentationRouter); err != nil {
			log.For(ctx).Fatal("Unable to register prometheus instrumentation", zap.Error(err))
		}
	}
	if app.Instrumentation.Jaeger.Enabled {
		cancelFunc, err := jaeger.RegisterExporter(ctx, app.Instrumentation.Jaeger.Config)
		if err != nil {
			log.For(ctx).Fatal("Unable to register jaeger instrumentation", zap.Error(err))
		}
		defer cancelFunc()
	}
	if app.Instrumentation.OCAgent.Enabled {
		cancelFunc, err := ocagent.RegisterExporter(ctx, app.Instrumentation.OCAgent.Config)
		if err != nil {
			log.For(ctx).Fatal("Unable to register ocagent instrumentation", zap.Error(err))
		}
		defer cancelFunc()
	}
	if app.Instrumentation.Runtime.Enabled {
		if err := runtime.Monitor(ctx, runtime.Config{
			Name:     app.Name,
			ID:       appID,
			Version:  app.Version,
			Revision: app.Revision,
			Interval: app.Instrumentation.Runtime.Config.Interval,
		}); err != nil {
			log.For(ctx).Fatal("Unable to start runtime monitoring", zap.Error(err))
		}
	}

	// Trace everything when debugging is enabled
	if app.Debug {
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	// Configure graceful restart
	upg, err := tableflip.New(tableflip.Options{})
	if err != nil {
		return xerrors.Errorf("platform: unable to register graceful restart handler: %w", err)
	}

	// Do an upgrade on SIGHUP
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			log.For(ctx).Info("Graceful reloading")
			_ = upg.Upgrade()
		}
	}()

	var group run.Group

	// Instrumentation server
	{
		ln, err := upg.Fds.Listen(app.Instrumentation.Network, app.Instrumentation.Listen)
		if err != nil {
			return xerrors.Errorf("platform: unable to start instrumentation server: %w", err)
		}

		server := &http.Server{
			Handler: instrumentationRouter,
		}

		group.Add(
			func() error {
				log.For(ctx).Info("Starting instrumentation server", zap.Stringer("address", ln.Addr()))
				return server.Serve(ln)
			},
			func(e error) {
				log.For(ctx).Info("Shutting instrumentation server down")

				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()

				log.CheckErrCtx(ctx, "Error raised while shutting down the server", server.Shutdown(ctx))
				log.SafeClose(server, "Unable to close instrumentation server")
			},
		)
	}

	// Initialize the component
	app.Builder(upg, &group)

	// Setup signal handler
	{
		var (
			cancelInterrupt = make(chan struct{})
			ch              = make(chan os.Signal, 2)
		)
		defer close(ch)

		group.Add(
			func() error {
				signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

				select {
				case sig := <-ch:
					log.For(ctx).Info("Captured signal", zap.Any("signal", sig))
				case <-cancelInterrupt:
				}

				return nil
			},
			func(e error) {
				close(cancelInterrupt)
				signal.Stop(ch)
			},
		)
	}

	// Final handler
	{
		group.Add(
			func() error {
				// Tell the parent we are ready
				_ = upg.Ready()

				// Wait for children to be ready
				// (or application shutdown)
				<-upg.Exit()

				return nil
			},
			func(e error) {
				upg.Stop()
			},
		)
	}

	// Run goroutine group
	return group.Run()
}
