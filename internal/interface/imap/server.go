// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package imap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"blackcat.ca/gmeow/internal/appsvc"
	"blackcat.ca/gmeow/internal/contracts"
)

type Server struct {
	services *appsvc.Services
	listener net.Listener
	address  string
	username string
	password string
}

func New(
	address, username, password string,
	services *appsvc.Services,
) (*Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("IMAP address is required")
	}
	if strings.TrimSpace(username) == "" {
		return nil, errors.New("IMAP username is required")
	}
	if services == nil {
		return nil, errors.New("IMAP app services are required")
	}

	return &Server{
		address:  address,
		username: username,
		password: password,
		services: services,
	}, nil
}

func (server *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", server.address)
	if err != nil {
		return err
	}
	server.listener = listener

	var wait sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			wait.Wait()
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return err
		}

		wait.Add(1)
		go func() {
			defer wait.Done()
			_ = server.handleConn(ctx, conn)
		}()
	}
}

func (server *Server) Addr() string {
	if server.listener == nil {
		return server.address
	}

	return server.listener.Addr().String()
}

func (server *Server) handleConn(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	state := imapState{}
	if _, err := fmt.Fprintln(writer, "* OK Gmeow read-only IMAP ready"); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		tag, command, rest := splitCommand(line)
		if tag == "" {
			continue
		}

		if err := server.handleCommand(ctx, writer, &state, tag, command, rest); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		if command == "LOGOUT" {
			return nil
		}
	}
}

type imapState struct {
	results []appsvc.ObjectSearchResult
}

func (server *Server) handleCommand(
	ctx context.Context,
	writer *bufio.Writer,
	state *imapState,
	tag string,
	command string,
	rest string,
) error {
	switch command {
	case "CAPABILITY":
		_, _ = fmt.Fprintln(writer, "* CAPABILITY IMAP4rev1 AUTH=PLAIN")
		_, _ = fmt.Fprintf(writer, "%s OK CAPABILITY completed\r\n", tag)
	case "LOGIN":
		user, password := parseLogin(rest)
		if user != server.username || (server.password != "" && password != server.password) {
			_, _ = fmt.Fprintf(writer, "%s NO authentication failed\r\n", tag)
			return nil
		}
		_, _ = fmt.Fprintf(writer, "%s OK LOGIN completed\r\n", tag)
	case "LIST":
		_, _ = fmt.Fprintln(writer, `* LIST (\HasNoChildren) "/" "INBOX"`)
		_, _ = fmt.Fprintf(writer, "%s OK LIST completed\r\n", tag)
	case "SELECT", "EXAMINE":
		results, err := server.services.ObjectSearch(ctx, appsvc.SearchOptions{
			Facets: []string{appsvc.MailMessageFacet},
			Limit:  100,
		})
		if err != nil {
			_, _ = fmt.Fprintf(writer, "%s NO %s\r\n", tag, err)
			return nil
		}
		state.results = results.Results
		_, _ = fmt.Fprintf(writer, "* %d EXISTS\r\n", len(state.results))
		_, _ = fmt.Fprintln(writer, "* FLAGS ()")
		_, _ = fmt.Fprintf(writer, "%s OK [READ-ONLY] %s completed\r\n", tag, command)
	case "SEARCH":
		query := strings.TrimSpace(rest)
		if strings.EqualFold(query, "ALL") {
			query = ""
		}
		results, err := server.services.ObjectSearch(
			ctx,
			appsvc.SearchOptions{
				Query:  query,
				Facets: []string{appsvc.MailMessageFacet},
				Limit:  100,
			},
		)
		if err != nil {
			_, _ = fmt.Fprintf(writer, "%s NO %s\r\n", tag, err)
			return nil
		}
		state.results = results.Results
		ids := make([]string, 0, len(state.results))
		for index := range state.results {
			ids = append(ids, strconv.Itoa(index+1))
		}
		_, _ = fmt.Fprintf(writer, "* SEARCH %s\r\n", strings.Join(ids, " "))
		_, _ = fmt.Fprintf(writer, "%s OK SEARCH completed\r\n", tag)
	case "FETCH":
		server.handleFetch(ctx, writer, state, tag, rest)
	case "NOOP":
		_, _ = fmt.Fprintf(writer, "%s OK NOOP completed\r\n", tag)
	case "LOGOUT":
		_, _ = fmt.Fprintln(writer, "* BYE Gmeow closing connection")
		_, _ = fmt.Fprintf(writer, "%s OK LOGOUT completed\r\n", tag)
	default:
		_, _ = fmt.Fprintf(writer, "%s BAD unsupported command\r\n", tag)
	}

	return nil
}

func (server *Server) handleFetch(
	ctx context.Context,
	writer *bufio.Writer,
	state *imapState,
	tag string,
	rest string,
) {
	fields := strings.Fields(rest)
	if len(fields) < 1 {
		_, _ = fmt.Fprintf(writer, "%s BAD FETCH requires sequence\r\n", tag)
		return
	}
	index, err := strconv.Atoi(fields[0])
	if err != nil || index <= 0 || index > len(state.results) {
		_, _ = fmt.Fprintf(writer, "%s NO no such message\r\n", tag)
		return
	}

	result := state.results[index-1]
	retrieved, err := server.services.Retrieve(ctx, result.ObjectDigest, true)
	if err != nil {
		_, _ = fmt.Fprintf(writer, "%s NO %s\r\n", tag, err)
		return
	}

	body := renderMessage(retrieved.Manifest, retrieved.Content)
	_, _ = fmt.Fprintf(
		writer,
		"* %d FETCH (UID %d RFC822.SIZE %d BODY[] {%d}\r\n%s\r\n)\r\n",
		index,
		stableUID(result.ObjectDigest),
		len(body),
		len(body),
		body,
	)
	_, _ = fmt.Fprintf(writer, "%s OK FETCH completed\r\n", tag)
}

func splitCommand(line string) (string, string, string) {
	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return "", "", ""
	}
	rest := ""
	if len(parts) == 3 {
		rest = parts[2]
	}

	return parts[0], strings.ToUpper(parts[1]), rest
}

func parseLogin(rest string) (string, string) {
	fields := strings.Fields(rest)
	if len(fields) < 2 {
		return "", ""
	}

	return strings.Trim(fields[0], `"`), strings.Trim(fields[1], `"`)
}

func renderMessage(manifest contracts.Manifest, content string) string {
	subject := "Gmeow object " + string(manifest.ObjectDigest)
	if len(manifest.Titles) > 0 && manifest.Titles[0].Value != "" {
		subject = manifest.Titles[0].Value
	}

	return fmt.Sprintf(
		"Subject: %s\r\nX-Gmeow-Digest: %s\r\nContent-Type: %s\r\n\r\n%s",
		subject,
		manifest.ObjectDigest,
		firstNonEmpty(manifest.MediaType, "text/plain"),
		content,
	)
}

func stableUID(digest contracts.ObjectDigest) int {
	value := 0
	for _, char := range string(digest) {
		value = (value*33 + int(char)) & 0x7fffffff
	}
	if value == 0 {
		return 1
	}

	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}
