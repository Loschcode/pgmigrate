package schemadiff

import (
	"strings"
	"testing"
)

func TestCanonical(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, definition, prefix, table string
		unique                          bool
		want                            string
	}{
		{
			name:       "plain",
			definition: "CREATE INDEX devices_idx ON app.devices USING btree (app_pk, user_id)",
			prefix:     "CREATE INDEX devices_idx ON ", table: "app.devices ",
			want: "USING btree (app_pk, user_id)",
		},
		{
			name:       "unique keeps uniqueness",
			definition: "CREATE UNIQUE INDEX k ON app.devices USING btree (id)",
			prefix:     "CREATE UNIQUE INDEX k ON ", table: "app.devices ", unique: true,
			want: "UNIQUE USING btree (id)",
		},
		{
			name:       "quoted names and ON ONLY",
			definition: `CREATE INDEX "odd index" ON ONLY "odd""schema"."odd table" USING btree ("odd col")`,
			prefix:     `CREATE INDEX "odd index" ON `, table: `"odd""schema"."odd table" `,
			want: `USING btree ("odd col")`,
		},
		{
			name:       "unexpected rendering is kept whole",
			definition: "CREATE INDEX x ON other.t USING btree (id)",
			prefix:     "CREATE INDEX x ON ", table: "app.t ",
			want: "CREATE INDEX x ON other.t USING btree (id)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := canonical(tc.definition, tc.prefix, tc.table, tc.unique); got != tc.want {
				t.Fatalf("canonical() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReplay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		index   catalogIndex
		missing map[string]bool
		want    []string
	}{
		{
			name:  "plain index builds concurrently",
			index: catalogIndex{Schema: "app", Table: "t", Name: "i", Definition: "CREATE INDEX i ON app.t USING btree (a)"},
			want:  []string{"CREATE INDEX CONCURRENTLY i ON app.t USING btree (a)"},
		},
		{
			name: "deferrable primary key",
			index: catalogIndex{
				Schema: "app", Table: "t", Name: "t_pkey", Definition: "CREATE UNIQUE INDEX t_pkey ON app.t USING btree (id)",
				ConstraintName: "t_pkey", ConstraintType: "p", ConstraintDeferrable: true,
			},
			want: []string{
				"CREATE UNIQUE INDEX CONCURRENTLY t_pkey ON app.t USING btree (id)",
				`ALTER TABLE "app"."t" ADD CONSTRAINT "t_pkey" PRIMARY KEY USING INDEX "t_pkey" DEFERRABLE INITIALLY IMMEDIATE`,
			},
		},
		{
			name: "partition index attaches to its parent",
			index: catalogIndex{
				Schema: "app", Table: "p1", Name: "p1_a_idx", Definition: "CREATE INDEX p1_a_idx ON app.p1 USING btree (a)",
				ParentSchema: "app", ParentName: "p_a_idx",
			},
			missing: map[string]bool{key("app", "p_a_idx"): true},
			want: []string{
				"CREATE INDEX CONCURRENTLY p1_a_idx ON app.p1 USING btree (a)",
				`ALTER INDEX "app"."p_a_idx" ATTACH PARTITION "app"."p1_a_idx"`,
			},
		},
		{
			name: "partitioned parent is created ON ONLY",
			index: catalogIndex{
				Schema: "app", Table: "p", Name: "p_a_idx", Definition: "CREATE INDEX p_a_idx ON ONLY app.p USING btree (a)",
				Partitioned: true,
			},
			want: []string{"CREATE INDEX p_a_idx ON ONLY app.p USING btree (a)"},
		},
		{
			name: "exclusion constraint is declared",
			index: catalogIndex{
				Schema: "app", Table: "t", Name: "t_excl", Definition: "CREATE INDEX t_excl ON app.t USING gist (r)",
				ConstraintName: "t_excl", ConstraintType: "x", ConstraintDefinition: "EXCLUDE USING gist (r WITH &&)",
			},
			want: []string{`ALTER TABLE "app"."t" ADD CONSTRAINT "t_excl" EXCLUDE USING gist (r WITH &&)`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := replay(tc.index, tc.missing).DDL
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("replay() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDescribeExplainsMissingTablesAndPrintsReplay(t *testing.T) {
	t.Parallel()
	result := Result{
		Tables: []Table{
			{Schema: "app", Name: "late", RelKind: "r"},
			{Schema: "app", Name: "events_10", RelKind: "r", Parent: "app.events"},
		},
		Indexes: []Index{{
			Schema: "app", Table: "devices", Name: "i", Definition: "CREATE INDEX i ON app.devices USING btree (a)",
			DDL: []string{"CREATE INDEX CONCURRENTLY i ON app.devices USING btree (a)"}, TargetInvalid: true,
		}},
	}
	got := result.Describe()
	for _, want := range []string{
		"target is missing 2 table(s) and 1 index(es)",
		`table "app"."late" is absent`,
		`partition "app"."events_10" of app.events`,
		"never replicated",
		"replay: CREATE INDEX CONCURRENTLY i ON app.devices USING btree (a);",
		"DROP INDEX CONCURRENTLY it first",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Describe() lacks %q:\n%s", want, got)
		}
	}
}
