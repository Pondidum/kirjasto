package main

import (
	"fmt"
	"kirjasto/command"
	"kirjasto/command/catalogue"
	"kirjasto/command/goes"
	"kirjasto/command/import_goodreads"
	"kirjasto/command/import_openlibrary"
	"kirjasto/command/library"
	"kirjasto/command/server"
	"kirjasto/command/version"
	"os"

	"github.com/hashicorp/cli"
)

func main() {

	defs := []command.CommandDefinition{
		version.NewVersionCommand(),
		server.NewServerCommand(),
		import_openlibrary.NewImportCommand(),
		import_goodreads.NewImportCommand(),
		catalogue.NewSearchCommand(),
		library.NewListCommand(),
		library.NewAddCommand(),
		library.NewStartCommand(),
		library.NewFinishCommand(),
		goes.NewGoesCommand(),
	}

	commands := make(map[string]cli.CommandFactory, len(defs))
	for _, def := range defs {
		commands[def.Name()] = command.NewCommand(def)
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
