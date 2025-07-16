package import_goodreads

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"kirjasto/config"
	"kirjasto/domain"
	"kirjasto/goes"
	"kirjasto/storage"
	"kirjasto/tracing"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tr = otel.Tracer("command.import.goodreads")

func NewImportCommand() *ImportCommand {
	return &ImportCommand{}
}

type ImportCommand struct {
}

func (c *ImportCommand) Synopsis() string {
	return "import goodreads library"
}

func (c *ImportCommand) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("import.goodreads", pflag.ContinueOnError)
	return flags
}

func (c *ImportCommand) Execute(ctx context.Context, config *config.Config, args []string) error {
	ctx, span := tr.Start(ctx, "execute")
	defer span.End()

	if len(args) != 1 {
		return tracing.Errorf(span, "this command takes exactly 1 argument: a path to import")
	}
	filePath := args[0]

	db, err := storage.Writer(ctx, config.DatabaseFile)
	if err != nil {
		return tracing.Error(span, err)
	}

	eventStore := goes.NewSqliteStore(db)
	if err := eventStore.Initialise(ctx); err != nil {
		return tracing.Error(span, err)
	}

	if err := goes.RegisterProjection("library_view", domain.NewLibraryProjection()); err != nil {
		return tracing.Error(span, err)
	}

	library, err := domain.LoadLibrary(ctx, eventStore, domain.LibraryID)
	if err != nil {
		if err != goes.ErrNotFound {
			return tracing.Error(span, err)
		}
		library = domain.NewLibrary(domain.LibraryID)
	}

	if err := processFile(ctx, library, filePath); err != nil {
		return tracing.Error(span, err)
	}

	if err := domain.SaveLibrary(ctx, eventStore, library); err != nil {
		return tracing.Error(span, err)
	}

	return nil
}

func readReviewsFile(ctx context.Context, exportFilePath string) (map[string]reviewEntry, error) {
	ctx, span := tr.Start(ctx, "read_reviews")
	defer span.End()

	reviewsFilePath := path.Join(path.Dir(exportFilePath), "review.json")

	file, err := os.Open(reviewsFilePath)
	if err != nil {
		return nil, tracing.Error(span, err)
	}
	defer file.Close()

	type reviewDto struct {
		Book       string
		ReadStatus string `json:"read_status"`
		UpdatedAt  string `json:"updated_at"`
	}

	var reviews []reviewDto
	if err := json.NewDecoder(file).Decode(&reviews); err != nil {
		return nil, tracing.Error(span, err)
	}

	lookup := make(map[string]reviewEntry, len(reviews))
	for _, review := range reviews {
		if review.Book != "" {

			ts, err := time.Parse("2006-01-02 15:04:05 MST", review.UpdatedAt)
			if err != nil {
				return nil, tracing.Error(span, err)
			}

			lookup[review.Book] = reviewEntry{
				Book:       review.Book,
				ReadStatus: review.ReadStatus,
				UpdatedAt:  ts,
			}
		}
	}

	return lookup, nil
}

type reviewEntry struct {
	Book       string
	ReadStatus string
	UpdatedAt  time.Time
}

func processFile(ctx context.Context, library *domain.Library, filePath string) error {
	ctx, span := tr.Start(ctx, "process_file")
	defer span.End()

	reviews, err := readReviewsFile(ctx, filePath)
	if err != nil {
		return tracing.Error(span, err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return tracing.Error(span, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// skip the headers
	if _, err = reader.Read(); err != nil {
		return tracing.Error(span, err)
	}

	lines := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return tracing.Error(span, err)
		}
		lines++

		if err := library.ImportBook(asBookImport(span, reviews, line)); err != nil {
			return tracing.Error(span, err)
		}
	}

	span.SetAttributes(attribute.Int("csv.lines", lines))

	return nil
}

func asBookImport(span trace.Span, reviews map[string]reviewEntry, line []string) domain.BookImport {
	isbns := make([]string, 0, 2)
	if isbn := line[fieldISBN13]; isbn != "" && isbn != `=""` {
		isbns = append(isbns, strings.TrimSuffix(strings.TrimPrefix(isbn, `="`), `"`))
	}
	if isbn := line[fieldISBN]; isbn != "" && isbn != `=""` {
		isbns = append(isbns, strings.TrimSuffix(strings.TrimPrefix(isbn, `="`), `"`))
	}

	rating := 0
	if val := line[fieldMyRating]; val != "" {
		i, err := strconv.Atoi(val)
		if err != nil {
			span.RecordError(fmt.Errorf("couldn't parse Rating: %w", err))
		} else {
			rating = i
		}
	}

	readCount := 0
	if val := line[fieldReadCount]; val != "" {
		i, err := strconv.Atoi(val)
		if err != nil {
			span.RecordError(fmt.Errorf("couldn't parse ReadCount: %w", err))
		} else {
			readCount = i
		}
	}

	shelves := []string{}
	if csv := line[fieldBookshelves]; csv != "" {
		vals := strings.Split(csv, ",")
		shelves = make([]string, len(vals))
		for i, val := range vals {
			shelves[i] = strings.TrimSpace(val)
		}
	}

	dateAdded := time.Time{}
	if val := line[fieldDateAdded]; val != "" {
		parsed, err := time.Parse("2006/01/02", val)
		if err != nil {
			span.RecordError(fmt.Errorf("couldn't parse DateAdded: %w", err))
		} else {
			dateAdded = parsed
		}
	}

	dateRead := time.Time{}
	if val := line[fieldDateRead]; val != "" {
		parsed, err := time.Parse("2006/01/02", val)
		if err != nil {
			span.RecordError(fmt.Errorf("couldn't parse DateRead: %w", err))
		} else {
			dateRead = parsed
		}
	}

	if readCount > 0 && dateRead.IsZero() {
		title := line[fieldTitle]
		if review, found := reviews[title]; found {
			span.AddEvent("review_lookup", trace.WithAttributes(attribute.String("book.title", title)))
			dateRead = review.UpdatedAt
		}
	}

	return domain.BookImport{
		Isbns:     isbns,
		Rating:    rating,
		ReadCount: readCount,
		Shelves:   shelves,
		DateAdded: dateAdded,
		DateRead:  dateRead,
	}

}

const (
	fieldBookId = iota
	fieldTitle
	fieldAuthor
	fieldAuthorLastFirst
	fieldAdditionalAuthors
	fieldISBN
	fieldISBN13
	fieldMyRating
	fieldAverageRating
	fieldPublisher
	fieldBinding
	fieldNumberOfPages
	fieldYearPublished
	fieldOriginalPublicationYear
	fieldDateRead
	fieldDateAdded
	fieldBookshelves
	fieldBookshelvesWithPositions
	fieldExclusiveShelf
	fieldMyReview
	fieldSpoiler
	fieldPrivateNotes
	fieldReadCount
	fieldOwnedCopies
)
