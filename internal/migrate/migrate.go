// Package migrate loads versioned schema migrations from files named
// NNNN_description.ext. Each database package applies them with its own
// runner and records applied versions in a schema_migrations table.
package migrate

import (
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Migration is one migration file.
type Migration struct {
	Version    int
	Name       string   // file name, e.g. 0001_create_links.cql
	Statements []string // in file order
}

var filePattern = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.(sql|cql)$`)

// Load reads every migration in dir of fsys, ordered by version. It rejects
// badly named files, duplicate versions and files without statements.
func Load(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	var migrations []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := filePattern.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q: name must be NNNN_description.sql or .cql", e.Name())
		}
		version, _ := strconv.Atoi(m[1])
		if version == 0 {
			return nil, fmt.Errorf("migration %q: versions start at 0001", e.Name())
		}
		if other, ok := seen[version]; ok {
			return nil, fmt.Errorf("migrations %q and %q have the same version", other, e.Name())
		}
		seen[version] = e.Name()

		data, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		statements := Split(string(data))
		if len(statements) == 0 {
			return nil, fmt.Errorf("migration %q has no statements", e.Name())
		}
		migrations = append(migrations, Migration{Version: version, Name: e.Name(), Statements: statements})
	}
	slices.SortFunc(migrations, func(a, b Migration) int { return a.Version - b.Version })
	return migrations, nil
}

// Split splits a migration file into statements on semicolons that end a
// line. Lines that are only "--" comments are dropped; statements must not
// contain semicolons inside string literals at the end of a line.
func Split(src string) []string {
	var statements []string
	var cur strings.Builder
	for line := range strings.Lines(src) {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		cur.WriteString(line)
		if strings.HasSuffix(trimmed, ";") {
			if s := strings.TrimSuffix(strings.TrimSpace(cur.String()), ";"); s != "" {
				statements = append(statements, s)
			}
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		statements = append(statements, s)
	}
	return statements
}

// Pending returns the migrations whose versions are not in applied.
func Pending(all []Migration, applied map[int]bool) []Migration {
	var pending []Migration
	for _, m := range all {
		if !applied[m.Version] {
			pending = append(pending, m)
		}
	}
	return pending
}
