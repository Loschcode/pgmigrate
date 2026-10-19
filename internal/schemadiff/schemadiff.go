// Package schemadiff finds the tables, partitions and indexes the source has and
// the target lacks.
//
// pgmigrate restores the schema once, from the snapshot taken at setup, and
// replicates DML only. DDL run on the source after that never reaches the
// target: an index built by an application migration or by hand during copy or
// follow, a table created by a migration, a partition attached to a migrated
// partitioned table. Nothing in the replication stream marks the gap, so a
// cutover would hand production a target without the object. This package is
// how cutover and schema-diff notice.
package schemadiff

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/GetStream/pgmigrate/internal/postgres"
)

// SelectTable reports whether a non-partition table belongs to the migration.
// It is the same predicate the run's table filter applies.
type SelectTable func(schema, table string) bool

// Table is a source table or partition the target does not have in the same
// form.
type Table struct {
	Schema  string `json:"schema"`
	Name    string `json:"name"`
	RelKind string `json:"relkind"`
	// TargetRelKind is the kind of the same-named target relation, empty when
	// the target has no relation of that name at all.
	TargetRelKind string `json:"target_relkind,omitempty"`
	// Parent names the partitioned table this one is a partition of.
	Parent string `json:"parent,omitempty"`
}

// Index is a valid source index with no equivalent valid target index.
type Index struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	Name       string `json:"name"`
	Definition string `json:"definition"`
	// DDL is what to run on the target to create it, empty when another
	// statement in the same result creates it.
	DDL []string `json:"ddl,omitempty"`
	// Note explains a DDL that is not a plain CREATE INDEX CONCURRENTLY.
	Note string `json:"note,omitempty"`
	// Parent is the partitioned index this one attaches to; its DDL runs first.
	Parent string `json:"parent,omitempty"`
	// TargetInvalid marks an invalid target index of the same name, a failed
	// build that has to be dropped before the DDL can run.
	TargetInvalid bool `json:"target_invalid,omitempty"`
	// TargetNameTaken marks a valid target index of the same name with another
	// definition, which the DDL would collide with.
	TargetNameTaken bool `json:"target_name_taken,omitempty"`
}

// Result lists every source object missing from the target.
type Result struct {
	Tables  []Table `json:"tables,omitempty"`
	Indexes []Index `json:"indexes,omitempty"`
	// Adopted lists expression or partial indexes matched by name rather than
	// by definition; see Diff.
	Adopted []string `json:"adopted,omitempty"`
}

// Missing reports whether the target lacks anything the source has.
func (r Result) Missing() bool { return len(r.Tables) > 0 || len(r.Indexes) > 0 }

// Summary is a one-line count of what is missing.
func (r Result) Summary() string {
	return fmt.Sprintf("target is missing %d table(s) and %d index(es) the source has", len(r.Tables), len(r.Indexes))
}

// Describe renders the findings for an operator, with the DDL to replay.
func (r Result) Describe() string {
	var b strings.Builder
	b.WriteString(r.Summary())
	b.WriteString("\n")
	for _, t := range r.Tables {
		name := quote(t.Schema, t.Name)
		switch {
		case t.TargetRelKind != "":
			fmt.Fprintf(&b, "  table %s is relkind %s on the source and %s on the target\n",
				name, relkindName(t.RelKind), relkindName(t.TargetRelKind))
		case t.Parent != "":
			fmt.Fprintf(&b, "  partition %s of %s is absent from the target\n", name, t.Parent)
		default:
			fmt.Fprintf(&b, "  table %s is absent from the target\n", name)
		}
	}
	var roots, partitions bool
	for _, t := range r.Tables {
		roots = roots || t.Parent == "" && t.TargetRelKind == ""
		partitions = partitions || t.Parent != "" && t.TargetRelKind == ""
	}
	if roots {
		b.WriteString("  A missing table's rows were never replicated: the publication is the table list fixed at setup,\n" +
			"  so creating it on the target now does not bring its data. Dump its definition with\n" +
			"  pg_dump --schema-only --table '<schema>.<table>' against the source, and copy or\n" +
			"  re-migrate its rows, before serving from the target.\n")
	}
	if partitions {
		b.WriteString("  A partition attached after setup is published under its own name, which the target lacks,\n" +
			"  so apply stops at the first row written to it: check the run log. Create it on the target\n" +
			"  (pg_dump --schema-only --table against the source) and attach it to its parent before resuming.\n")
	}
	for _, x := range r.Indexes {
		fmt.Fprintf(&b, "  index %s on %s is absent from the target\n    source: %s\n",
			quote(x.Schema, x.Name), quote(x.Schema, x.Table), x.Definition)
		if x.TargetInvalid {
			fmt.Fprintf(&b, "    note: the target has an invalid index of this name, a failed build; DROP INDEX CONCURRENTLY it first\n")
		}
		if x.TargetNameTaken {
			fmt.Fprintf(&b, "    note: the target has a different index of this name; rename or drop it first\n")
		}
		if x.Note != "" {
			fmt.Fprintf(&b, "    note: %s\n", x.Note)
		}
		for _, statement := range x.DDL {
			fmt.Fprintf(&b, "    replay: %s;\n", statement)
		}
	}
	return b.String()
}

// sourceRelation is an in-scope source table or partition.
type sourceRelation struct {
	OID                   uint32
	Schema, Name, RelKind string
	Parent                string
}

// Diff compares the target's tables, partitions and valid indexes against the
// source's current catalog, scoped to the tables selected selects and the
// partitions under them now.
//
// Scope comes from the source as it is now, not from the setup snapshot,
// because the objects this exists to find were created after it.
//
// Indexes are compared by definition, not by name: pg_get_indexdef with the
// index name and the qualified table stripped, keyed by table. A target index
// under another name with the same definition is the same index for every
// query the planner will run. Both sessions pin an empty search_path first,
// since the deparser qualifies a function, operator class or type only when
// its schema is not on the session's path.
//
// The exception is an index with an expression key or a partial predicate.
// PostgreSQL stores those as parse trees and can deparse an index rebuilt from
// its own rendering differently (the reason indexbuild adopts them by name), so
// one that finds no definition match is still accepted when the same table has
// a valid expression or partial index of the same name. It is listed under
// Adopted, so the operator can see the match was by name.
func Diff(ctx context.Context, source, target *pgx.Conn, selects SelectTable) (Result, error) {
	if err := postgres.PinSearchPath(ctx, source); err != nil {
		return Result{}, err
	}
	if err := postgres.PinSearchPath(ctx, target); err != nil {
		return Result{}, err
	}
	relations, err := sourceRelations(ctx, source, selects)
	if err != nil {
		return Result{}, err
	}
	var result Result
	present, err := targetRelations(ctx, target, relations)
	if err != nil {
		return Result{}, err
	}
	for _, relation := range relations {
		kind, ok := present[key(relation.Schema, relation.Name)]
		if ok && kind == relation.RelKind {
			continue
		}
		result.Tables = append(result.Tables, Table{
			Schema: relation.Schema, Name: relation.Name, RelKind: relation.RelKind,
			TargetRelKind: kind, Parent: relation.Parent,
		})
	}

	sourceIndexes, err := readIndexes(ctx, source, relations, true)
	if err != nil {
		return Result{}, fmt.Errorf("read source indexes: %w", err)
	}
	targetIndexes, err := readIndexes(ctx, target, relations, false)
	if err != nil {
		return Result{}, fmt.Errorf("read target indexes: %w", err)
	}
	absentTables := make(map[string]bool, len(result.Tables))
	for _, table := range result.Tables {
		absentTables[key(table.Schema, table.Name)] = true
	}
	// Matching is by count: two source indexes with one definition need two on
	// the target, and one target index cannot satisfy both.
	byDefinition := make(map[string]int, len(targetIndexes))
	byName := make(map[string]catalogIndex, len(targetIndexes))
	invalid := make(map[string]bool)
	for _, x := range targetIndexes {
		if !x.Valid {
			invalid[key(x.Schema, x.Name)] = true
			continue
		}
		byDefinition[key(x.Schema, x.Table, x.Canonical)]++
		byName[key(x.Schema, x.Table, x.Name)] = x
	}
	var unmatched []catalogIndex
	for _, x := range sourceIndexes {
		if absentTables[key(x.Schema, x.Table)] {
			// Reported with its table; there is nothing to build it on.
			continue
		}
		definition := key(x.Schema, x.Table, x.Canonical)
		if byDefinition[definition] > 0 {
			byDefinition[definition]--
			continue
		}
		unmatched = append(unmatched, x)
	}
	missing := make(map[string]bool)
	var absent []catalogIndex
	for _, x := range unmatched {
		if x.Expression {
			same, ok := byName[key(x.Schema, x.Table, x.Name)]
			if ok && same.Expression && byDefinition[key(same.Schema, same.Table, same.Canonical)] > 0 {
				byDefinition[key(same.Schema, same.Table, same.Canonical)]--
				result.Adopted = append(result.Adopted, quote(x.Schema, x.Name))
				continue
			}
		}
		missing[key(x.Schema, x.Name)] = true
		absent = append(absent, x)
	}
	for _, x := range absent {
		index := replay(x, missing)
		index.TargetInvalid = invalid[key(x.Schema, x.Name)]
		_, index.TargetNameTaken = byName[key(x.Schema, x.Table, x.Name)]
		result.Indexes = append(result.Indexes, index)
	}
	return result, nil
}

func sourceRelations(ctx context.Context, source *pgx.Conn, selects SelectTable) ([]sourceRelation, error) {
	rows, err := source.Query(ctx, `
		SELECT c.oid, n.nspname, c.relname, c.relkind::text
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname !~ '^pg_toast' AND NOT c.relispartition
		ORDER BY n.nspname, c.relname`)
	if err != nil {
		return nil, fmt.Errorf("inventory source tables: %w", err)
	}
	relations, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceRelation, error) {
		var r sourceRelation
		err := row.Scan(&r.OID, &r.Schema, &r.Name, &r.RelKind)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("inventory source tables: %w", err)
	}
	selected := relations[:0]
	var roots []uint32
	for _, relation := range relations {
		if selects == nil || selects(relation.Schema, relation.Name) {
			selected = append(selected, relation)
			if relation.RelKind == "p" {
				roots = append(roots, relation.OID)
			}
		}
	}
	if len(roots) == 0 {
		return selected, nil
	}
	// The same descent the schema phase makes, but over the catalog as it is
	// now: a partition attached since setup is exactly what this looks for.
	rows, err = source.Query(ctx, `
		WITH RECURSIVE descendants(oid, parent) AS (
			SELECT h.inhrelid, h.inhparent FROM pg_catalog.pg_inherits h
			WHERE h.inhparent = ANY($1::oid[])
			UNION
			SELECT h.inhrelid, h.inhparent
			FROM pg_catalog.pg_inherits h JOIN descendants d ON d.oid=h.inhparent
		)
		SELECT c.oid, n.nspname, c.relname, c.relkind::text,
		       quote_ident(pn.nspname)||'.'||quote_ident(p.relname)
		FROM descendants d
		JOIN pg_catalog.pg_class c ON c.oid=d.oid AND c.relispartition
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		JOIN pg_catalog.pg_class p ON p.oid=d.parent
		JOIN pg_catalog.pg_namespace pn ON pn.oid=p.relnamespace
		WHERE c.relkind IN ('r','p')
		ORDER BY n.nspname, c.relname`, roots)
	if err != nil {
		return nil, fmt.Errorf("inventory source partitions: %w", err)
	}
	partitions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceRelation, error) {
		var r sourceRelation
		err := row.Scan(&r.OID, &r.Schema, &r.Name, &r.RelKind, &r.Parent)
		return r, err
	})
	if err != nil {
		return nil, fmt.Errorf("inventory source partitions: %w", err)
	}
	return append(selected, partitions...), nil
}

func targetRelations(ctx context.Context, target *pgx.Conn, relations []sourceRelation) (map[string]string, error) {
	schemas, names := relationNames(relations)
	rows, err := target.Query(ctx, `
		SELECT n.nspname, c.relname, c.relkind::text
		FROM unnest($1::text[], $2::text[]) AS wanted(nspname, relname)
		JOIN pg_catalog.pg_namespace n ON n.nspname=wanted.nspname
		JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=wanted.relname`,
		schemas, names)
	if err != nil {
		return nil, fmt.Errorf("inventory target tables: %w", err)
	}
	present := make(map[string]string, len(relations))
	var schema, name, kind string
	_, err = pgx.ForEachRow(rows, []any{&schema, &name, &kind}, func() error {
		present[key(schema, name)] = kind
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inventory target tables: %w", err)
	}
	return present, nil
}

type catalogIndex struct {
	Schema, Table, Name, Definition, Prefix string
	Canonical                               string
	Valid, Expression, Partitioned          bool
	// ParentSchema and ParentName name the partitioned index this one is
	// attached to.
	ParentSchema, ParentName string
	// ConstraintName, ConstraintType and ConstraintDefinition describe the
	// primary key, unique or exclusion constraint the index backs.
	ConstraintName, ConstraintType, ConstraintDefinition string
	ConstraintDeferrable, ConstraintInitiallyDeferred    bool
}

// readIndexes reads every index on the named relations. On the source only
// valid, ready indexes count: an invalid one is a failed or in-progress build
// that the planner does not use either. Target indexes are all returned with
// their validity, and only valid ones satisfy a source index.
func readIndexes(ctx context.Context, conn *pgx.Conn, relations []sourceRelation, onlyValid bool) ([]catalogIndex, error) {
	schemas, names := relationNames(relations)
	// prefix is what pg_get_indexdef renders before the access method: CREATE
	// [UNIQUE] INDEX <name> ON [ONLY] <schema>.<table>, built from the same
	// quote_ident the deparser uses so that quoted names compare exactly.
	rows, err := conn.Query(ctx, `
		SELECT nt.nspname, t.relname, x.relname, pg_catalog.pg_get_indexdef(i.indexrelid),
		       'CREATE '||CASE WHEN i.indisunique THEN 'UNIQUE ' ELSE '' END||'INDEX '||
		         quote_ident(x.relname)||' ON ',
		       quote_ident(nt.nspname)||'.'||quote_ident(t.relname)||' ',
		       i.indisvalid AND i.indisready AND i.indislive, i.indexprs IS NOT NULL OR i.indpred IS NOT NULL,
		       x.relkind='I', coalesce(np.nspname,''), coalesce(xp.relname,''),
		       coalesce(c.conname,''), coalesce(c.contype::text,''),
		       coalesce(pg_catalog.pg_get_constraintdef(c.oid),''),
		       coalesce(c.condeferrable,false), coalesce(c.condeferred,false), i.indisunique
		FROM unnest($1::text[], $2::text[]) AS wanted(nspname, relname)
		JOIN pg_catalog.pg_namespace nt ON nt.nspname=wanted.nspname
		JOIN pg_catalog.pg_class t ON t.relnamespace=nt.oid AND t.relname=wanted.relname
		JOIN pg_catalog.pg_index i ON i.indrelid=t.oid
		JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid
		LEFT JOIN pg_catalog.pg_inherits h ON h.inhrelid=x.oid
		LEFT JOIN pg_catalog.pg_class xp ON xp.oid=h.inhparent
		LEFT JOIN pg_catalog.pg_namespace np ON np.oid=xp.relnamespace
		LEFT JOIN pg_catalog.pg_constraint c ON c.conindid=i.indexrelid AND c.conrelid=t.oid
		  AND c.contype IN ('p','u','x')
		ORDER BY nt.nspname, t.relname, x.relname`, schemas, names)
	if err != nil {
		return nil, err
	}
	var indexes []catalogIndex
	var x catalogIndex
	var prefix, table string
	var unique bool
	_, err = pgx.ForEachRow(rows, []any{&x.Schema, &x.Table, &x.Name, &x.Definition, &prefix, &table,
		&x.Valid, &x.Expression, &x.Partitioned, &x.ParentSchema, &x.ParentName,
		&x.ConstraintName, &x.ConstraintType, &x.ConstraintDefinition,
		&x.ConstraintDeferrable, &x.ConstraintInitiallyDeferred, &unique}, func() error {
		if onlyValid && !x.Valid {
			return nil
		}
		x.Canonical = canonical(x.Definition, prefix, table, unique)
		indexes = append(indexes, x)
		return nil
	})
	return indexes, err
}

// canonical strips the index name and the qualified table from a
// pg_get_indexdef rendering, leaving "[UNIQUE ]USING method (keys) …". A
// rendering that does not have the expected prefix is kept whole: it can then
// only match an identical rendering, which errs toward reporting it missing.
func canonical(definition, prefix, table string, unique bool) string {
	rest, ok := strings.CutPrefix(definition, prefix)
	if !ok {
		return definition
	}
	rest = strings.TrimPrefix(rest, "ONLY ")
	rest, ok = strings.CutPrefix(rest, table)
	if !ok {
		return definition
	}
	if unique {
		return "UNIQUE " + rest
	}
	return rest
}

// replay renders the statements that recreate x on the target, in the order
// they must run, following the sequence indexbuild itself uses: a partitioned
// parent's index is created ON ONLY, which is instant and leaves it invalid,
// and each partition's index is built CONCURRENTLY and attached to it, which
// validates the parent once the last one is attached. Nothing but an
// exclusion constraint takes a lock that blocks writes for the length of a
// build.
func replay(x catalogIndex, missing map[string]bool) Index {
	result := Index{Schema: x.Schema, Table: x.Table, Name: x.Name, Definition: x.Definition}
	table := quote(x.Schema, x.Table)
	switch {
	case x.ConstraintType == "x":
		// An exclusion constraint has no USING INDEX form, so it is declared
		// directly and builds its index under the constraint's lock.
		result.DDL = []string{fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
			table, quote(x.ConstraintName), x.ConstraintDefinition)}
		result.Note = "declaring an exclusion constraint builds its index while blocking writes to the table"
		return result
	case x.Partitioned && x.ConstraintType != "":
		// A partitioned table rejects ADD CONSTRAINT … USING INDEX; declared
		// on ONLY the parent, the constraint's index starts out invalid like
		// any other ON ONLY index.
		result.DDL = []string{fmt.Sprintf("ALTER TABLE ONLY %s ADD CONSTRAINT %s %s",
			table, quote(x.ConstraintName), x.ConstraintDefinition)}
	case x.Partitioned:
		result.DDL = []string{x.Definition}
	default:
		result.DDL = []string{concurrently(x.Definition)}
	}
	if x.Partitioned {
		result.Note = "partitioned index: valid once every partition's index is attached to it"
	}
	// A partition's index that backs its own share of the parent's constraint
	// has to be a constraint index before PostgreSQL lets it attach.
	if !x.Partitioned && (x.ConstraintType == "p" || x.ConstraintType == "u") {
		kind := "UNIQUE"
		if x.ConstraintType == "p" {
			kind = "PRIMARY KEY"
		}
		result.DDL = append(result.DDL, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s USING INDEX %s%s",
			table, quote(x.ConstraintName), kind, quote(x.Name), x.deferrability()))
	}
	if x.ParentName != "" {
		result.Parent = quote(x.ParentSchema, x.ParentName)
		result.DDL = append(result.DDL, fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s",
			quote(x.ParentSchema, x.ParentName), quote(x.Schema, x.Name)))
		if missing[key(x.ParentSchema, x.ParentName)] {
			result.Note = strings.TrimPrefix(result.Note+"; attaches to "+result.Parent+
				", which is missing too: create that one first", "; ")
		}
	}
	return result
}

func (x catalogIndex) deferrability() string {
	switch {
	case !x.ConstraintDeferrable:
		return ""
	case x.ConstraintInitiallyDeferred:
		return " DEFERRABLE INITIALLY DEFERRED"
	default:
		return " DEFERRABLE INITIALLY IMMEDIATE"
	}
}

func concurrently(definition string) string {
	for _, head := range []string{"CREATE UNIQUE INDEX ", "CREATE INDEX "} {
		if rest, ok := strings.CutPrefix(definition, head); ok {
			return head + "CONCURRENTLY " + rest
		}
	}
	return definition
}

func relationNames(relations []sourceRelation) ([]string, []string) {
	schemas := make([]string, len(relations))
	names := make([]string, len(relations))
	for i, relation := range relations {
		schemas[i], names[i] = relation.Schema, relation.Name
	}
	return schemas, names
}

func key(parts ...string) string { return strings.Join(parts, "\x00") }

func quote(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

func relkindName(kind string) string {
	switch kind {
	case "r":
		return "r (table)"
	case "p":
		return "p (partitioned table)"
	case "v":
		return "v (view)"
	case "m":
		return "m (materialized view)"
	case "f":
		return "f (foreign table)"
	default:
		return kind
	}
}

// Sort orders a result deterministically, for output and for tests.
func (r *Result) Sort() {
	sort.Slice(r.Tables, func(i, j int) bool {
		return key(r.Tables[i].Schema, r.Tables[i].Name) < key(r.Tables[j].Schema, r.Tables[j].Name)
	})
	// A partitioned index comes before every partition index that attaches to
	// it, at any depth, so the replay statements can be run top to bottom.
	parents := make(map[string]string, len(r.Indexes))
	for _, x := range r.Indexes {
		parents[quote(x.Schema, x.Name)] = x.Parent
	}
	depth := func(x Index) int {
		n := 0
		for parent := x.Parent; parent != "" && n < len(parents); parent = parents[parent] {
			n++
		}
		return n
	}
	sort.SliceStable(r.Indexes, func(i, j int) bool {
		if di, dj := depth(r.Indexes[i]), depth(r.Indexes[j]); di != dj {
			return di < dj
		}
		return key(r.Indexes[i].Schema, r.Indexes[i].Table, r.Indexes[i].Name) <
			key(r.Indexes[j].Schema, r.Indexes[j].Table, r.Indexes[j].Name)
	})
	sort.Strings(r.Adopted)
}
