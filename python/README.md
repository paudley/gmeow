# gmeow-intel

`gmeow-intel` is the Python ANALYSIS package for the Go rewrite. It is the only planned PyPI
package in the greenfield architecture.

The Go core owns FILESTORE, QUERY, SCHEDULER, SOURCE, and INTERFACE runtime behavior. Python workers
receive resolved analyzer/job payloads through the worker channel and write FILESTORE annotations;
they do not parse `gmeow.toml`, open SOPS files, or start the core service.
