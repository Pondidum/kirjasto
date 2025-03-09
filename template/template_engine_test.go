package template

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const expected = `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Catalogue - Kirjasto</title>

    <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  </head>
  <body>
    <main id="main">
<h1>This is a list of all books available</h1>
<ol>
  <li>one</li>
  <li>two</li>
  <li>three</li>
</ol>

    </main>
  </body>
</html>
`

func TestTemplateEngineParsing(t *testing.T) {

	te := NewTemplateEngine(os.DirFS("../ui").(FS), EngineOptions{HotReload: false})
	assert.NoError(t, te.ParseTemplates(context.Background()))

	content := &bytes.Buffer{}

	assert.NoError(t, te.Render(context.Background(), "catalogue/catalogue.html", map[string]any{}, content))
	assert.Equal(t, expected, content.String())

}
