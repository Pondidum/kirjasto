package importcmd

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
)

type validatedFile struct {
	lines int
}

type recordProcessed struct {
	changed bool
	err     error
}

type fileImported struct {
}

type model struct {
	err       error
	total     int
	updated   int
	processed int
}

func (m *model) Init() tea.Cmd {
	return nil
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		}

	case validatedFile:
		m.total = msg.lines

	case recordProcessed:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		if msg.changed {
			m.updated++
		}
		m.processed++

	case fileImported:
		return m, tea.Quit

	}

	return m, nil
}

func (m *model) View() string {
	if m.total == 0 {
		return "validating file...\n"
	}

	return fmt.Sprintf("validating file...Done\nTotal lines: %v\nProcessed: %v\nChanged: %v\n", m.total, m.processed, m.updated)
}
