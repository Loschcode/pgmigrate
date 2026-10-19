package cutover

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/schemadiff"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5"
)

func clean(context.Context) (schemadiff.Result, error) { return schemadiff.Result{}, nil }

type resumeState struct {
	mu          sync.Mutex
	steps       map[string]bool
	details     map[string]string
	endPosition string
	failReport  bool
}

func (s *resumeState) StepCompleted(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.steps[name], nil
}

func (s *resumeState) CompleteStep(_ context.Context, name, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == stepReport && s.failReport {
		s.failReport = false
		return errors.New("injected marker failure")
	}
	if s.steps[name] {
		return nil
	}
	s.steps[name] = true
	if s.details == nil {
		s.details = map[string]string{}
	}
	s.details[name] = detail
	return nil
}

func (s *resumeState) SetEndPosition(_ context.Context, position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endPosition = position
	return nil
}

func (s *resumeState) Migration(context.Context) (state.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return state.Migration{EndPosition: s.endPosition}, nil
}

func (s *resumeState) ListSteps(context.Context) ([]state.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []state.Step
	for name, done := range s.steps {
		result = append(result, state.Step{Name: name, Completed: done, Detail: s.details[name]})
	}
	return result, nil
}

func TestReportResumesAfterMarkerFailure(t *testing.T) {
	t.Parallel()
	steps := map[string]bool{
		stepEndPosition: true, stepDrain: true,
		stepSequences: true, stepCleanup: true,
	}
	store := &resumeState{steps: steps, endPosition: "0/123", failReport: true}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	connector := func(context.Context) (*pgx.Conn, error) {
		t.Fatal("completed step unexpectedly opened PostgreSQL")
		return nil, nil
	}
	cfg := Config{
		Source: connector, Target: connector, State: store, Dir: t.TempDir(),
		EmitBoundary: func(context.Context) (string, error) {
			t.Fatal("a recorded end position was unexpectedly emitted again")
			return "", nil
		},
		WaitDrain: func(context.Context, string) error {
			t.Fatal("completed drain unexpectedly reran")
			return nil
		},
		Cleanup: func(context.Context) error {
			t.Fatal("completed cleanup unexpectedly reran")
			return nil
		},
		Now:          func() time.Time { return now },
		SchemaParity: clean,
		ToolVersion:  "v1.2.3",
		AuditConfig:  map[string]string{"migration": "test"},
	}
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("expected injected report marker failure")
	}
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resume cutover: %v", err)
	}
	if !report.CompletedAt.Equal(now) || report.EndPosition != "0/123" {
		t.Fatalf("resumed report = %#v", report)
	}
	if report.ToolVersion != "v1.2.3" || report.Configuration.Values["migration"] != "test" {
		t.Fatalf("report metadata = %#v", report)
	}
	if done, _ := store.StepCompleted(context.Background(), stepReport); !done {
		t.Fatal("report marker was not completed on resume")
	}
}

// The boundary is emitted once and then reused for the life of the migration.
// A second attempt that emitted a fresh one would move the line between what
// this cutover migrated and what it abandoned, after the target had already been
// drained to the first.
func TestResumeReusesTheBoundaryTheFirstAttemptEmitted(t *testing.T) {
	t.Parallel()
	// Sequences and cleanup start marked done because they need PostgreSQL and this
	// test is about the boundary. Nothing before them is marked, so both attempts
	// run the end-position and drain steps for real.
	store := &resumeState{steps: map[string]bool{stepSequences: true, stepCleanup: true}}
	emitted := 0
	drained := false
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(),
		EmitBoundary: func(context.Context) (string, error) {
			emitted++
			return "0/500", nil
		},
		WaitDrain: func(context.Context, string) error {
			if !drained {
				return errors.New("apply has not reached the end position")
			}
			return nil
		},
		Cleanup:      func(context.Context) error { return nil },
		SchemaParity: clean,
	}
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("expected the first attempt to fail draining")
	}
	drained = true
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resume cutover: %v", err)
	}
	if emitted != 1 {
		t.Errorf("the boundary was emitted %d times, want once", emitted)
	}
	if report.EndPosition != "0/500" {
		t.Errorf("end position = %q, want the emitted 0/500", report.EndPosition)
	}
}

type phaseStore struct {
	resumeState
	phase       state.Phase
	transitions []state.Phase
}

func (s *phaseStore) Migration(context.Context) (state.Migration, error) {
	return state.Migration{EndPosition: "0/123", Phase: s.phase}, nil
}

func (s *phaseStore) TransitionPhase(_ context.Context, phase state.Phase) error {
	s.phase = phase
	s.transitions = append(s.transitions, phase)
	return nil
}

func TestLifecycleTransitionsAndReportConfiguration(t *testing.T) {
	t.Parallel()
	store := &phaseStore{
		resumeState: resumeState{
			steps:       map[string]bool{stepEndPosition: true, stepSequences: true, stepCleanup: true},
			endPosition: "0/123",
		},
		phase: state.PhaseFollow,
	}
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(), ToolVersion: "test-version",
		AuditConfig:  map[string]string{"profile": "safe"},
		WaitDrain:    func(context.Context, string) error { return nil },
		Cleanup:      func(context.Context) error { return nil },
		SchemaParity: clean,
	}
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []state.Phase{state.PhaseDrained, state.PhaseCutover, state.PhaseComplete}
	if len(store.transitions) != len(want) {
		t.Fatalf("transitions = %v", store.transitions)
	}
	for i := range want {
		if store.transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", store.transitions, want)
		}
	}
	if report.ToolVersion != "test-version" || report.Configuration.Values["profile"] != "safe" {
		t.Fatalf("report configuration = %#v", report)
	}
}

var missingIndex = schemadiff.Result{Indexes: []schemadiff.Index{{
	Schema: "app", Table: "devices", Name: "devices_app_pk_user_id_non_disabled",
	Definition: "CREATE INDEX devices_app_pk_user_id_non_disabled ON app.devices USING btree (app_pk, user_id) WHERE (NOT disabled)",
}}}

// schemaConfig is a cutover whose steps after the parity check fail loudly or
// are observable, so a test can tell how far an attempt got.
func schemaConfig(t *testing.T, store State, parity func(context.Context) (schemadiff.Result, error)) (*Config, *int) {
	t.Helper()
	emitted := 0
	refuse := func(context.Context) (*pgx.Conn, error) { return nil, errors.New("sequences reached") }
	return &Config{
		Source: refuse, Target: refuse, State: store, Dir: t.TempDir(),
		EmitBoundary: func(context.Context) (string, error) {
			emitted++
			return "0/900", nil
		},
		WaitDrain:    func(context.Context, string) error { return nil },
		Cleanup:      func(context.Context) error { t.Fatal("cleanup reached"); return nil },
		SchemaParity: parity,
	}, &emitted
}

// A refused cutover must leave nothing durable behind. In particular no end
// position: one recorded here would be reused by the next attempt, after the
// operator had unfrozen writes to build what was missing, and every write made
// in between would be left behind on the source.
func TestSchemaParityRefusalLeavesNoTrace(t *testing.T) {
	t.Parallel()
	store := &phaseStore{resumeState: resumeState{steps: map[string]bool{}}, phase: state.PhaseFollow}
	cfg, emitted := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) { return missingIndex, nil })
	cfg.WaitDrain = func(context.Context, string) error { t.Fatal("drain reached"); return nil }
	cfg.Prepare = func(context.Context) error { t.Fatal("prepare reached"); return nil }
	_, err := Run(context.Background(), *cfg)
	var missing *MissingObjectsError
	if !errors.As(err, &missing) || len(missing.Result.Indexes) != 1 {
		t.Fatalf("Run() error = %v, want a MissingObjectsError naming the index", err)
	}
	if *emitted != 0 || store.endPosition != "" {
		t.Fatalf("refusal emitted %d boundaries, recorded end position %q", *emitted, store.endPosition)
	}
	if len(store.steps) != 0 || len(store.transitions) != 0 {
		t.Fatalf("refusal recorded steps %v and transitions %v", store.steps, store.transitions)
	}
}

func TestSchemaParityRerunAfterFixPasses(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{}}
	result := missingIndex
	cfg, emitted := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) { return result, nil })
	if _, err := Run(context.Background(), *cfg); !errors.As(err, new(*MissingObjectsError)) {
		t.Fatalf("first attempt error = %v, want a refusal", err)
	}
	result = schemadiff.Result{}
	if _, err := Run(context.Background(), *cfg); err == nil || !strings.Contains(err.Error(), "sequences reached") {
		t.Fatalf("second attempt error = %v, want it to proceed to sequences", err)
	}
	if !store.steps[stepSchema] || !store.steps[stepDrain] || *emitted != 1 {
		t.Fatalf("steps = %v, boundaries emitted = %d", store.steps, *emitted)
	}
}

func TestSchemaParityOverrideProceedsAndRecords(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{}}
	checks := 0
	cfg, _ := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) {
		checks++
		return missingIndex, nil
	})
	cfg.AllowMissingObjects = true
	if _, err := Run(context.Background(), *cfg); err == nil || !strings.Contains(err.Error(), "sequences reached") {
		t.Fatalf("overridden attempt error = %v, want it to proceed to sequences", err)
	}
	if !strings.Contains(store.details[stepSchema], "devices_app_pk_user_id_non_disabled") {
		t.Fatalf("schema step detail = %q, want the acknowledged index", store.details[stepSchema])
	}
	// Finish the cutover; the resumed attempt keeps the recorded verdict and
	// puts the acknowledged objects in the report.
	store.steps[stepSequences], store.steps[stepCleanup] = true, true
	cfg.Cleanup = func(context.Context) error { return nil }
	report, err := Run(context.Background(), *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 {
		t.Fatalf("parity checked %d times, want once", checks)
	}
	if report.MissingObjects == nil || len(report.MissingObjects.Indexes) != 1 {
		t.Fatalf("report missing objects = %#v", report.MissingObjects)
	}
}

func TestSchemaParityDoesNotRerun(t *testing.T) {
	t.Parallel()
	for name, steps := range map[string]map[string]bool{
		// A verdict already recorded stands.
		"recorded": {stepSchema: true, stepEndPosition: true, stepDrain: true, stepSequences: true, stepCleanup: true},
		// A cutover started by a release without the check is past the point
		// where refusing could prevent anything.
		"past sequences": {stepEndPosition: true, stepDrain: true, stepSequences: true, stepCleanup: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := &resumeState{steps: steps, endPosition: "0/1"}
			cfg, _ := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) {
				t.Fatal("schema parity rechecked")
				return schemadiff.Result{}, nil
			})
			cfg.Cleanup = func(context.Context) error { return nil }
			if _, err := Run(context.Background(), *cfg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A cutover begun by a release without the check can already have an end
// position. Refusing then cannot undo it, so the refusal has to say that the
// next attempt reuses it and writes must stay frozen.
func TestSchemaParityRefusalNamesARecordedEndPosition(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{stepEndPosition: true}, endPosition: "0/77"}
	cfg, _ := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) { return missingIndex, nil })
	_, err := Run(context.Background(), *cfg)
	var missing *MissingObjectsError
	if !errors.As(err, &missing) || !strings.Contains(err.Error(), "0/77") || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestPrepareRunsAfterTheCheckAndBeforeTheEndPosition(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{}}
	var order []string
	cfg, _ := schemaConfig(t, store, func(context.Context) (schemadiff.Result, error) {
		order = append(order, "check")
		return schemadiff.Result{}, nil
	})
	cfg.Prepare = func(context.Context) error {
		order = append(order, "prepare")
		store.endPosition = "0/42"
		return nil
	}
	cfg.EmitBoundary = func(context.Context) (string, error) {
		t.Fatal("an end position set by Prepare was not used")
		return "", nil
	}
	_, _ = Run(context.Background(), *cfg)
	if strings.Join(order, ",") != "check,prepare" || store.endPosition != "0/42" {
		t.Fatalf("order = %v, end position = %q", order, store.endPosition)
	}
}
