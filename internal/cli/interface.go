// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/config"
	imapiface "blackcat.ca/gmeow/internal/interface/imap"
	mcpiface "blackcat.ca/gmeow/internal/interface/mcp"
	"blackcat.ca/gmeow/internal/interface/rest"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/source/sourcegrpc"
)

func newMCPServeCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "mcp-serve",
		Short: "Run the MCP interface over stdio",
		RunE: func(command *cobra.Command, _ []string) error {
			services, closeFn, err := openInterfaceServices(command.Context(), configPath)
			if err != nil {
				return err
			}
			defer closeFn()

			server, err := mcpiface.New(services)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintln(out, "mcp serve: stdio")

			return server.Start(command.Context())
		},
	}
}

func newMCPHTTPServeCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "mcp-http-serve",
		Short: "Run the MCP interface over Streamable HTTP",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			iface, err := interfaceByKindWithAddress(loaded.Config.Interfaces, "mcp")
			if err != nil {
				return err
			}
			sessionTimeout, err := interfaceSessionTimeout(iface)
			if err != nil {
				return err
			}
			services, closeFn, err := openInterfaceServicesLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			server, err := mcpiface.NewHTTP(
				interfaceAddress(iface),
				services,
				mcpiface.HTTPOptions{SessionTimeout: sessionTimeout},
			)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "mcp http serve: %s\n", interfaceAddress(iface))

			return server.Start(command.Context())
		},
	}
}

func newRESTServeCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "rest-serve",
		Short: "Run the REST interface",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			iface, err := interfaceByKind(loaded.Config.Interfaces, "rest")
			if err != nil {
				return err
			}
			services, closeFn, err := openInterfaceServicesLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			server, err := rest.New(interfaceAddress(iface), services)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "rest serve: %s\n", interfaceAddress(iface))

			return server.Start(command.Context())
		},
	}
}

func newIMAPServeCommand(out io.Writer, configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "imap-serve",
		Short: "Run the read-only IMAP interface",
		RunE: func(command *cobra.Command, _ []string) error {
			loaded, err := config.Load(config.Options{Path: *configPath})
			if err != nil {
				return err
			}
			iface, err := interfaceByKind(loaded.Config.Interfaces, "imap")
			if err != nil {
				return err
			}
			services, closeFn, err := openInterfaceServicesLoaded(command.Context(), loaded)
			if err != nil {
				return err
			}
			defer closeFn()

			server, err := imapiface.New(
				interfaceAddress(iface),
				iface.Username,
				iface.Password,
				services,
			)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "imap serve: %s\n", interfaceAddress(iface))

			return server.Start(command.Context())
		},
	}
}

func openInterfaceServices(
	ctx context.Context,
	configPath *string,
) (*appsvc.Services, func(), error) {
	loaded, err := config.Load(config.Options{Path: *configPath})
	if err != nil {
		return nil, nil, err
	}

	return openInterfaceServicesLoaded(ctx, loaded)
}

func openInterfaceServicesLoaded(
	ctx context.Context,
	loaded *config.Loaded,
) (*appsvc.Services, func(), error) {
	filestoreClient, err := rpc.NewFilestoreClient(
		ctx,
		rpcEndpoint(loaded.Resolved.RPC.Filestore),
	)
	if err != nil {
		return nil, nil, err
	}

	queryClient, err := rpc.NewQueryClient(ctx, rpcEndpoint(loaded.Resolved.RPC.Query))
	if err != nil {
		_ = filestoreClient.Close()

		return nil, nil, err
	}

	schedulerClient, err := rpc.NewSchedulerClient(
		ctx,
		rpcEndpoint(loaded.Resolved.RPC.Scheduler),
	)
	if err != nil {
		_ = queryClient.Close()
		_ = filestoreClient.Close()

		return nil, nil, err
	}

	sourceService, err := source.NewService(filestoreClient)
	if err != nil {
		_ = schedulerClient.Close()
		_ = queryClient.Close()
		_ = filestoreClient.Close()

		return nil, nil, err
	}
	sourceRegistry, closeSources, err := buildSourceRegistry(commandContext(ctx), loaded)
	if err != nil {
		_ = schedulerClient.Close()
		_ = queryClient.Close()
		_ = filestoreClient.Close()

		return nil, nil, err
	}

	services, err := appsvc.New(appsvc.Options{
		Query:      queryClient,
		Objects:    filestoreClient,
		Scheduler:  schedulerClient,
		Sources:    sourceRegistry,
		Ingest:     sourceService,
		Operations: queryClient,
	})
	if err != nil {
		closeSources()
		_ = schedulerClient.Close()
		_ = queryClient.Close()
		_ = filestoreClient.Close()

		return nil, nil, err
	}

	return services, func() {
		closeSources()
		_ = schedulerClient.Close()
		_ = queryClient.Close()
		_ = filestoreClient.Close()
	}, nil
}

func interfaceByKind(
	interfaces []config.InterfaceConfig,
	kind string,
) (config.InterfaceConfig, error) {
	for _, iface := range interfaces {
		if iface.Kind == kind {
			return iface, nil
		}
	}

	return config.InterfaceConfig{}, fmt.Errorf("no %s interface is configured", kind)
}

func interfaceByKindWithAddress(
	interfaces []config.InterfaceConfig,
	kind string,
) (config.InterfaceConfig, error) {
	for _, iface := range interfaces {
		if strings.TrimSpace(iface.Kind) == kind && iface.Host != "" && iface.Port != 0 {
			return iface, nil
		}
	}

	return config.InterfaceConfig{}, fmt.Errorf(
		"no addressed %s interface is configured",
		kind,
	)
}

func interfaceAddress(iface config.InterfaceConfig) string {
	return net.JoinHostPort(iface.Host, fmt.Sprint(iface.Port))
}

func interfaceSessionTimeout(iface config.InterfaceConfig) (time.Duration, error) {
	if strings.TrimSpace(iface.SessionTimeout) == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(iface.SessionTimeout)
	if err != nil {
		return 0, fmt.Errorf("interface %q session_timeout: %w", iface.Name, err)
	}

	return duration, nil
}

func buildSourceRegistry(
	ctx context.Context,
	loaded *config.Loaded,
) (*appsvc.StaticSourceRegistry, func(), error) {
	adapters := []source.Adapter{}
	clients := []*sourcegrpc.Client{}
	for index, sourceConfig := range loaded.Config.Sources {
		if sourceConfig.Kind != "gmail" {
			continue
		}
		client, err := sourcegrpc.NewClient(
			ctx,
			rpcEndpoint(loaded.Resolved.Sources[index].Endpoint),
			sourceConfig.Kind,
			sourceConfig.Name,
			sourceConfig.Capabilities,
		)
		if err != nil {
			for _, existing := range clients {
				_ = existing.Close()
			}

			return nil, nil, err
		}
		clients = append(clients, client)
		adapters = append(adapters, client)
	}

	return appsvc.NewStaticSourceRegistry(adapters...), func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}, nil
}

func buildSourceAdapters(
	ctx context.Context,
	loaded *config.Loaded,
) ([]source.Adapter, error) {
	adapters := []source.Adapter{}
	for _, sourceConfig := range loaded.Config.Sources {
		switch sourceConfig.Kind {
		case "gmail":
			if sourceConfig.CredentialSecret == "" {
				continue
			}
			backend, err := source.NewGoogleGmailBackend(
				ctx,
				[]byte(loaded.Config.Secrets[sourceConfig.CredentialSecret]),
				sourceConfig.UserID,
				sourceConfig.DelegatedSubject,
			)
			if err != nil {
				return nil, err
			}
			adapter, err := source.NewGmailAdapter(sourceConfig.Name, backend)
			if err != nil {
				return nil, err
			}
			adapters = append(adapters, adapter)
		}
	}

	return adapters, nil
}

func commandContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}

	return context.Background()
}
