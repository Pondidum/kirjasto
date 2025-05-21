package main

import (
	"fmt"
	"kirjasto/command"
	"kirjasto/command/import_goodreads"
	"kirjasto/command/import_openlibrary"
	"kirjasto/command/server"
	"kirjasto/command/version"
	"os"

	"github.com/hashicorp/cli"
)

func main() {

	commands := map[string]cli.CommandFactory{
		"version":            command.NewCommand(version.NewVersionCommand()),
		"server":             command.NewCommand(server.NewServerCommand()),
		"import openlibrary": command.NewCommand(import_openlibrary.NewImportCommand()),
		"import goodreads":   command.NewCommand(import_goodreads.NewImportCommand()),
	}

	cli := &cli.CLI{
		Name:                       "kirjasto",
		Args:                       os.Args[1:],
		Commands:                   commands,
		Autocomplete:               true,
		AutocompleteNoDefaultFlags: false,
	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
	}

	os.Exit(exitCode)
}
