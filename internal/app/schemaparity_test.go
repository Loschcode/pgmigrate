package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/state"
)

func TestMigrationFilterResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	rules := "app.*\n!app.excluded\n"
	filter, err := config.ParseFilter(strings.NewReader(rules))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := loadFilter("")
	if err != nil {
		t.Fatal(err)
	}
	filterFile := filepath.Join(t.TempDir(), "filter")
	if err := os.WriteFile(filterFile, []byte("# comment\n"+rules), 0o600); err != nil {
		t.Fatal(err)
	}
	otherFile := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(otherFile, []byte("app.*\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	open := func(t *testing.T, fingerprint string) *state.Store {
		store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: fingerprint})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		return store
	}

	t.Run("recorded rules need no flag", func(t *testing.T) {
		store := open(t, filter.Fingerprint())
		if err := recordFilterRules(ctx, store, filter); err != nil {
			t.Fatal(err)
		}
		got, err := migrationFilter(ctx, store, "")
		if err != nil {
			t.Fatal(err)
		}
		if !got.Match("app", "devices") || got.Match("app", "excluded") {
			t.Fatalf("recovered filter %q selects the wrong tables", got.Normalized())
		}
	})
	t.Run("unfiltered migration without recorded rules", func(t *testing.T) {
		store := open(t, empty.Fingerprint())
		got, err := migrationFilter(ctx, store, "")
		if err != nil {
			t.Fatal(err)
		}
		if !got.Match("anything", "at_all") {
			t.Fatal("empty filter did not select everything")
		}
	})
	t.Run("filtered migration without recorded rules needs the same filter", func(t *testing.T) {
		store := open(t, filter.Fingerprint())
		if _, err := migrationFilter(ctx, store, ""); err == nil || !strings.Contains(err.Error(), "--table-filter") {
			t.Fatalf("no filter: error = %v", err)
		}
		if _, err := migrationFilter(ctx, store, otherFile); err == nil {
			t.Fatal("a different filter was accepted")
		}
		got, err := migrationFilter(ctx, store, filterFile)
		if err != nil {
			t.Fatal(err)
		}
		if got.Match("app", "excluded") {
			t.Fatal("supplied filter not applied")
		}
	})
}
