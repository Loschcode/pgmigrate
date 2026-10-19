//go:build integration

package schemadiff

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/GetStream/pgmigrate/internal/pgtest"
)

// baseline is the schema both servers start from: what the schema and index
// phases would have left on the target.
const baseline = `
	CREATE SCHEMA app;
	CREATE SCHEMA "odd""schema";
	CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;
	CREATE TABLE app.devices (
		id bigint PRIMARY KEY, app_pk int NOT NULL, user_id text NOT NULL,
		disabled boolean NOT NULL DEFAULT false, name text);
	CREATE INDEX devices_app_pk_user_id_idx ON app.devices (app_pk, user_id);
	CREATE INDEX devices_lower_name ON app.devices (lower(name)) WHERE NOT disabled;
	CREATE INDEX devices_name_trgm ON app.devices USING gin (name public.gin_trgm_ops);
	CREATE INDEX devices_covering ON app.devices (app_pk DESC NULLS LAST, name COLLATE "C") INCLUDE (user_id) WITH (fillfactor = 80);
	CREATE UNIQUE INDEX devices_nnd ON app.devices (name) NULLS NOT DISTINCT;
	CREATE TABLE app.events (id bigint, at date NOT NULL, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
	CREATE TABLE app.events_2026_09 PARTITION OF app.events FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
	CREATE INDEX events_at_idx ON app.events (at);
	CREATE TABLE "odd""schema"."odd table" ("odd col" int);
	CREATE TABLE app.excluded (id int);`

type pair struct {
	source, target *pgx.Conn
}

func start(t *testing.T, major int) pair {
	t.Helper()
	ctx := context.Background()
	source := pgtest.Start(t, major).Connect(t)
	target := pgtest.Start(t, major).Connect(t)
	for _, conn := range []*pgx.Conn{source, target} {
		if _, err := conn.Exec(ctx, baseline); err != nil {
			t.Fatal(err)
		}
	}
	return pair{source: source, target: target}
}

func (p pair) diff(t *testing.T) Result {
	t.Helper()
	result, err := Diff(context.Background(), p.source, p.target, func(schema, table string) bool {
		return table != "excluded"
	})
	if err != nil {
		t.Fatal(err)
	}
	result.Sort()
	return result
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func indexNames(result Result) []string {
	var names []string
	for _, x := range result.Indexes {
		names = append(names, x.Schema+"."+x.Name)
	}
	return names
}

func tableNames(result Result) []string {
	var names []string
	for _, x := range result.Tables {
		names = append(names, x.Schema+"."+x.Name)
	}
	return names
}

func TestPG17IdenticalSchemasHaveNoFindings(t *testing.T) {
	p := start(t, 17)
	// A session search_path that differs between the two servers changes how
	// pg_get_indexdef qualifies public.gin_trgm_ops; Diff pins both.
	exec(t, p.target, "SET search_path = app, public")
	exec(t, p.source, "SET search_path = public")
	if result := p.diff(t); result.Missing() || len(result.Adopted) > 0 {
		t.Fatalf("identical schemas reported %s", result.Describe())
	}
}

func TestPG17IndexAddedOnSourceIsMissing(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "CREATE INDEX devices_app_pk_user_id_non_disabled ON app.devices (app_pk, user_id) WHERE NOT disabled")
	result := p.diff(t)
	if got := indexNames(result); len(got) != 1 || got[0] != "app.devices_app_pk_user_id_non_disabled" {
		t.Fatalf("missing indexes = %v\n%s", got, result.Describe())
	}
	want := `CREATE INDEX CONCURRENTLY devices_app_pk_user_id_non_disabled ON app.devices USING btree (app_pk, user_id) WHERE (NOT disabled)`
	if ddl := result.Indexes[0].DDL; len(ddl) != 1 || ddl[0] != want {
		t.Fatalf("replay DDL = %q, want %q", ddl, want)
	}
	if len(result.Tables) != 0 {
		t.Fatalf("missing tables = %v", tableNames(result))
	}

	// Replaying the printed DDL on the target clears the finding.
	exec(t, p.target, result.Indexes[0].DDL[0])
	if result := p.diff(t); result.Missing() {
		t.Fatalf("after replay: %s", result.Describe())
	}
}

func TestPG17RenamedEquivalentIndexIsPresent(t *testing.T) {
	p := start(t, 17)
	exec(t, p.target, "ALTER INDEX app.devices_app_pk_user_id_idx RENAME TO device_users")
	if result := p.diff(t); result.Missing() || len(result.Adopted) > 0 {
		t.Fatalf("renamed equivalent reported %s", result.Describe())
	}
}

func TestPG17SameNameDifferentDefinitionIsMissing(t *testing.T) {
	p := start(t, 17)
	exec(t, p.target, `DROP INDEX app.devices_app_pk_user_id_idx;
		CREATE INDEX devices_app_pk_user_id_idx ON app.devices (app_pk)`)
	result := p.diff(t)
	if got := indexNames(result); len(got) != 1 || got[0] != "app.devices_app_pk_user_id_idx" {
		t.Fatalf("missing indexes = %v\n%s", got, result.Describe())
	}
}

func TestPG17InvalidIndexes(t *testing.T) {
	p := start(t, 17)
	// An invalid source index is a failed build the planner ignores, so the
	// target need not have it; an invalid target index does not count.
	for _, conn := range []*pgx.Conn{p.source, p.target} {
		exec(t, conn, "CREATE INDEX devices_user_id ON app.devices (user_id)")
	}
	exec(t, p.target, `UPDATE pg_index SET indisvalid=false WHERE indexrelid='app.devices_user_id'::regclass`)
	exec(t, p.source, "CREATE INDEX devices_name ON app.devices (name)")
	exec(t, p.source, `UPDATE pg_index SET indisvalid=false WHERE indexrelid='app.devices_name'::regclass`)
	result := p.diff(t)
	if got := indexNames(result); len(got) != 1 || got[0] != "app.devices_user_id" {
		t.Fatalf("missing indexes = %v\n%s", got, result.Describe())
	}
}

func TestPG17TableAndPartitionAddedOnSourceAreMissing(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, `
		CREATE TABLE app.retention_backup_logs (id bigint PRIMARY KEY);
		CREATE TABLE app.events_2026_10 PARTITION OF app.events FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
		CREATE TABLE app.excluded_too (id int);
		CREATE TABLE app.excluded_new (id int)`)
	result := p.diff(t)
	got := strings.Join(tableNames(result), ",")
	// excluded_too is selected by the filter (only "excluded" is not), so it
	// is reported; the filter's exclusions never are.
	if got != "app.events_2026_10,app.excluded_new,app.excluded_too,app.retention_backup_logs" {
		t.Fatalf("missing tables = %s\n%s", got, result.Describe())
	}
	for _, table := range result.Tables {
		if table.Name == "events_2026_10" && table.Parent != "app.events" {
			t.Fatalf("partition parent = %q", table.Parent)
		}
	}
	// The new partition's own indexes are missing too; they come from the
	// parent's partitioned indexes, so the replay is the partition itself.
	if !strings.Contains(result.Describe(), "never replicated") {
		t.Fatalf("description does not explain that rows were not replicated:\n%s", result.Describe())
	}
}

func TestPG17FilterExcludedTableIsIgnored(t *testing.T) {
	p := start(t, 17)
	exec(t, p.target, "DROP TABLE app.excluded")
	exec(t, p.source, "CREATE INDEX excluded_id ON app.excluded (id)")
	if result := p.diff(t); result.Missing() {
		t.Fatalf("filter-excluded table reported %s", result.Describe())
	}
}

func TestPG17RelkindMismatchIsMissing(t *testing.T) {
	p := start(t, 17)
	exec(t, p.target, `DROP TABLE "odd""schema"."odd table"; CREATE VIEW "odd""schema"."odd table" AS SELECT 1 AS "odd col"`)
	result := p.diff(t)
	if len(result.Tables) != 1 || result.Tables[0].TargetRelKind != "v" || result.Tables[0].Name != "odd table" {
		t.Fatalf("tables = %#v", result.Tables)
	}
}

func TestPG17QuotedNamesCompareByDefinition(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, `CREATE INDEX "odd index" ON "odd""schema"."odd table" ("odd col")`)
	exec(t, p.target, `CREATE INDEX "other name" ON "odd""schema"."odd table" ("odd col")`)
	if result := p.diff(t); result.Missing() {
		t.Fatalf("quoted equivalent reported %s", result.Describe())
	}
	exec(t, p.source, `CREATE INDEX "odd unique" ON "odd""schema"."odd table" ("odd col") WHERE "odd col" > 0`)
	result := p.diff(t)
	if got := indexNames(result); len(got) != 1 || got[0] != `odd"schema.odd unique` {
		t.Fatalf("missing indexes = %v\n%s", got, result.Describe())
	}
}

func TestPG17UniquenessIsPartOfTheDefinition(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "CREATE UNIQUE INDEX devices_user_unique ON app.devices (app_pk, user_id, id)")
	exec(t, p.target, "CREATE INDEX devices_user_unique ON app.devices (app_pk, user_id, id)")
	result := p.diff(t)
	if got := indexNames(result); len(got) != 1 || got[0] != "app.devices_user_unique" {
		t.Fatalf("missing indexes = %v\n%s", got, result.Describe())
	}
}

func TestPG17ConstraintIndexReplay(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "ALTER TABLE app.devices ADD CONSTRAINT devices_user_key UNIQUE (app_pk, user_id)")
	result := p.diff(t)
	if len(result.Indexes) != 1 {
		t.Fatalf("missing indexes = %v", indexNames(result))
	}
	ddl := result.Indexes[0].DDL
	want := []string{
		"CREATE UNIQUE INDEX CONCURRENTLY devices_user_key ON app.devices USING btree (app_pk, user_id)",
		`ALTER TABLE "app"."devices" ADD CONSTRAINT "devices_user_key" UNIQUE USING INDEX "devices_user_key"`,
	}
	if strings.Join(ddl, ";") != strings.Join(want, ";") {
		t.Fatalf("replay DDL = %q, want %q", ddl, want)
	}
	for _, statement := range ddl {
		exec(t, p.target, statement)
	}
	if result := p.diff(t); result.Missing() {
		t.Fatalf("after replay: %s", result.Describe())
	}
}

func TestPG17PartitionedIndexReplay(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "CREATE INDEX events_id_idx ON app.events (id)")
	result := p.diff(t)
	// The parent's index is created ON ONLY and the partition's is built
	// concurrently and attached, so nothing blocks writes for a build.
	if got := strings.Join(indexNames(result), ","); got != "app.events_id_idx,app.events_2026_09_id_idx" {
		t.Fatalf("missing indexes = %s\n%s", got, result.Describe())
	}
	want := []string{
		"CREATE INDEX events_id_idx ON ONLY app.events USING btree (id)",
		"CREATE INDEX CONCURRENTLY events_2026_09_id_idx ON app.events_2026_09 USING btree (id)",
		`ALTER INDEX "app"."events_id_idx" ATTACH PARTITION "app"."events_2026_09_id_idx"`,
	}
	var got []string
	for _, x := range result.Indexes {
		got = append(got, x.DDL...)
	}
	if strings.Join(got, ";") != strings.Join(want, ";") {
		t.Fatalf("replay DDL = %q, want %q", got, want)
	}
	for _, statement := range got {
		exec(t, p.target, statement)
	}
	if result := p.diff(t); result.Missing() {
		t.Fatalf("after replay: %s", result.Describe())
	}
}

func TestPG17PartitionedConstraintReplay(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "ALTER TABLE app.events ADD CONSTRAINT events_at_id_key UNIQUE (at, id)")
	result := p.diff(t)
	if got := strings.Join(indexNames(result), ","); got != "app.events_at_id_key,app.events_2026_09_at_id_key" {
		t.Fatalf("missing indexes = %s\n%s", got, result.Describe())
	}
	for _, x := range result.Indexes {
		for _, statement := range x.DDL {
			exec(t, p.target, statement)
		}
	}
	if result := p.diff(t); result.Missing() {
		t.Fatalf("after replay: %s", result.Describe())
	}
	var valid bool
	if err := p.target.QueryRow(context.Background(),
		`SELECT indisvalid FROM pg_index WHERE indexrelid='app.events_at_id_key'::regclass`).Scan(&valid); err != nil || !valid {
		t.Fatalf("replayed partitioned constraint index valid=%v err=%v", valid, err)
	}
}

func TestPG17MissingTableHidesItsIndexes(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "CREATE TABLE app.late (id bigint PRIMARY KEY, v text); CREATE INDEX late_v ON app.late (v)")
	result := p.diff(t)
	if len(result.Indexes) != 0 || strings.Join(tableNames(result), ",") != "app.late" {
		t.Fatalf("result = %s", result.Describe())
	}
}

func TestPG17DuplicateDefinitionsAreCounted(t *testing.T) {
	p := start(t, 17)
	exec(t, p.source, "CREATE INDEX devices_app_pk_user_id_dup ON app.devices (app_pk, user_id)")
	result := p.diff(t)
	if len(result.Indexes) != 1 {
		t.Fatalf("missing indexes = %v", indexNames(result))
	}
}

func TestPG17ExpressionIndexAdoptedByName(t *testing.T) {
	p := start(t, 17)
	// The same index name with a predicate that renders differently, as an
	// expression index rebuilt from another server's rendering can.
	exec(t, p.target, `DROP INDEX app.devices_lower_name;
		CREATE INDEX devices_lower_name ON app.devices (lower(name)) WHERE disabled IS NOT TRUE`)
	result := p.diff(t)
	if result.Missing() || len(result.Adopted) != 1 {
		t.Fatalf("result = %s adopted=%v", result.Describe(), result.Adopted)
	}
}

func TestPG17InvalidTargetIndexIsFlagged(t *testing.T) {
	p := start(t, 17)
	exec(t, p.target, `UPDATE pg_index SET indisvalid=false WHERE indexrelid='app.devices_app_pk_user_id_idx'::regclass`)
	result := p.diff(t)
	if len(result.Indexes) != 1 || !result.Indexes[0].TargetInvalid {
		t.Fatalf("result = %#v", result.Indexes)
	}
	if !strings.Contains(result.Describe(), "DROP INDEX CONCURRENTLY it first") {
		t.Fatal(result.Describe())
	}
}

func TestCrossMajorIdenticalSchemas(t *testing.T) {
	majors := pgtest.Majors(t)
	if len(majors) < 2 {
		t.Skip("set PGTEST_MAJORS to two majors, for example 16,18")
	}
	ctx := context.Background()
	source := pgtest.Start(t, majors[0]).Connect(t)
	target := pgtest.Start(t, majors[len(majors)-1]).Connect(t)
	for _, conn := range []*pgx.Conn{source, target} {
		if _, err := conn.Exec(ctx, baseline); err != nil {
			t.Fatal(err)
		}
	}
	result, err := Diff(ctx, source, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Missing() {
		t.Fatalf("PostgreSQL %d to %d reported %s", majors[0], majors[len(majors)-1], result.Describe())
	}
}
