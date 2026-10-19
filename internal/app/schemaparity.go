package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/schemadiff"
	"github.com/GetStream/pgmigrate/internal/state"
)

// stepFilterRules records the run's normalized table filter. The migration
// identity keeps only its fingerprint, which is enough to refuse a different
// filter but not to apply this one to a table that did not exist when the run
// started, and finding such tables is what the schema parity check is for.
const stepFilterRules = "filter.rules"

// errSchemaDrift is schema-diff's non-zero exit for a target missing objects.
var errSchemaDrift = errors.New("target is missing tables or indexes the source has")

// recordFilterRules stores the filter the store was opened with. CompleteStep
// keeps the first detail it is given, and Open has already refused a filter with
// a different fingerprint, so the recorded rules always hash to the
// migration's filter fingerprint.
func recordFilterRules(ctx context.Context, store *state.Store, filter config.Filter) error {
	return store.CompleteStep(ctx, stepFilterRules, filter.Normalized())
}

// migrationFilter recovers the table filter the migration runs with, so the
// schema parity check scopes the source the way the run does. A migration
// whose run predates the recorded rules has only the fingerprint: an empty
// filter is recognized by it, and anything else needs --table-filter with the
// same rules.
func migrationFilter(ctx context.Context, store *state.Store, path string) (config.Filter, error) {
	migration, err := store.Migration(ctx)
	if err != nil {
		return config.Filter{}, err
	}
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return config.Filter{}, err
	}
	for _, step := range steps {
		if step.Name != stepFilterRules || !step.Completed {
			continue
		}
		filter, err := config.ParseFilter(strings.NewReader(step.Detail))
		if err != nil {
			return config.Filter{}, fmt.Errorf("decode recorded table filter: %w", err)
		}
		if filter.Fingerprint() != migration.FilterFingerprint {
			return config.Filter{}, errors.New("recorded table filter does not match the migration's filter fingerprint")
		}
		return filter, nil
	}
	filter, err := loadFilter(path)
	if err != nil {
		return config.Filter{}, err
	}
	if filter.Fingerprint() != migration.FilterFingerprint {
		return config.Filter{}, errors.New("this migration was started with a table filter that its state does not record; " +
			"pass the same --table-filter it was started with, so the schema check covers the same tables")
	}
	return filter, nil
}

// schemaParity returns the check cutover and schema-diff run: the source's
// tables, partitions and valid indexes for the migration's filter, against the
// target's.
func schemaParity(cfg config.Config, filter config.Filter) func(context.Context) (schemadiff.Result, error) {
	return func(ctx context.Context) (schemadiff.Result, error) {
		source, err := postgres.Connect(ctx, cfg.Source)
		if err != nil {
			return schemadiff.Result{}, err
		}
		defer source.Close(context.Background())
		target, err := postgres.Connect(ctx, cfg.Target)
		if err != nil {
			return schemadiff.Result{}, err
		}
		defer target.Close(context.Background())
		result, err := schemadiff.Diff(ctx, source, target, filter.Match)
		result.Sort()
		return result, err
	}
}

// SchemaDiff reports the tables, partitions and indexes the source has and the
// target lacks, the check cutover refuses on, without cutting over. It reads
// both catalogs and nothing else, so it is safe to run during follow, which is
// when to run it: an index found missing only at cutover is built while the
// application's writes are frozen.
func (a App) SchemaDiff(ctx context.Context, cfg config.Config) error {
	store, err := state.OpenControl(ctx, cfg.Dir)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := validateTargetIdentity(ctx, cfg, store); err != nil {
		return err
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	switch migration.Phase {
	case state.PhasePreflight, state.PhaseSetup, state.PhaseSchema, state.PhaseCopy, state.PhaseIndexes:
		return fmt.Errorf("schema-diff compares a restored target and needs the catchup phase or later, current phase is %s", migration.Phase)
	}
	filter, err := migrationFilter(ctx, store, cfg.TableFilter)
	if err != nil {
		return err
	}
	result, err := schemaParity(cfg, filter)(ctx)
	if err != nil {
		return err
	}
	if result.Missing() {
		fmt.Fprint(a.progressOutput(), result.Describe())
	}
	if err := json.NewEncoder(a.output()).Encode(result); err != nil {
		return err
	}
	if result.Missing() {
		return fmt.Errorf("%w: %s", errSchemaDrift, result.Summary())
	}
	return nil
}
