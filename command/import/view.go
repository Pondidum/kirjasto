package importcmd

import (
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
)

type fileInfo struct {
	Type        string
	RecordCount int
}

type fileVerified struct {
	recordCount int
}

type recordProcessed struct {
	changed bool
	err     error
}

type ftsCreationStarted struct{}
type fileImported struct {
}

type model struct {
	fileType string
	total    int
	updated  float64
	records  progress.Model
	fts      spinner.Model
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

	case fileInfo:
		m.total = msg.RecordCount
		m.fileType = msg.Type

	case recordProcessed:
		m.updated++
		// m.records.SetPercent()

	case fileImported:
		return m, tea.Quit

	case ftsCreationStarted:
		m.fts.Tick()
		return m, m.fts.Tick

	default:
		var cmd tea.Cmd
		m.fts, cmd = m.fts.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m *model) View() string {

	if m.updated >= float64(m.total) {
		return m.fts.View() + " Creating FTS indexes"
	}

	return m.records.ViewAs(m.updated / float64(m.total))
}
