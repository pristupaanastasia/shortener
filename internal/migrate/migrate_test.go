package migrate

import (
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/pristupaanastasia/shortener/migrations"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name         string
		files        fstest.MapFS
		wantVersions []int
		wantErr      bool
	}{
		{
			name: "ordered by version",
			files: fstest.MapFS{
				"db/0002_second.sql": {Data: []byte("CREATE TABLE b (id int);")},
				"db/0001_first.sql":  {Data: []byte("CREATE TABLE a (id int);")},
			},
			wantVersions: []int{1, 2},
		},
		{name: "empty directory", files: fstest.MapFS{"db": {Mode: fs.ModeDir}}, wantVersions: nil},
		{name: "duplicate version", wantErr: true, files: fstest.MapFS{
			"db/0001_a.sql": {Data: []byte("SELECT 1;")},
			"db/0001_b.sql": {Data: []byte("SELECT 2;")},
		}},
		{name: "bad name", wantErr: true, files: fstest.MapFS{"db/create_users.sql": {Data: []byte("SELECT 1;")}}},
		{name: "uppercase name", wantErr: true, files: fstest.MapFS{"db/0001_Create.sql": {Data: []byte("SELECT 1;")}}},
		{name: "version zero", wantErr: true, files: fstest.MapFS{"db/0000_init.sql": {Data: []byte("SELECT 1;")}}},
		{name: "only comments", wantErr: true, files: fstest.MapFS{"db/0001_empty.sql": {Data: []byte("-- nothing here\n")}}},
		{name: "missing directory", wantErr: true, files: fstest.MapFS{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Load(tt.files, "db")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Load err = %v, want error: %v", err, tt.wantErr)
			}
			var versions []int
			for _, m := range got {
				versions = append(versions, m.Version)
			}
			if !slices.Equal(versions, tt.wantVersions) {
				t.Errorf("versions = %v, want %v", versions, tt.wantVersions)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{name: "one statement", src: "CREATE TABLE a (id int);\n", want: []string{"CREATE TABLE a (id int)"}},
		{
			name: "multi-line statements and comments",
			src:  "-- header\nCREATE TABLE a (\n    id int -- inline\n);\n\n-- next\nCREATE INDEX i ON a (id);\n",
			want: []string{"CREATE TABLE a (\n    id int -- inline\n)", "CREATE INDEX i ON a (id)"},
		},
		{name: "no trailing semicolon", src: "SELECT 1", want: []string{"SELECT 1"}},
		{name: "blank", src: "\n\n", want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Split(tt.src); !slices.Equal(got, tt.want) {
				t.Errorf("Split(%q) = %q, want %q", tt.src, got, tt.want)
			}
		})
	}
}

func TestPending(t *testing.T) {
	all := []Migration{{Version: 1}, {Version: 2}, {Version: 3}}
	tests := []struct {
		name    string
		applied map[int]bool
		want    []int
	}{
		{name: "fresh database", applied: map[int]bool{}, want: []int{1, 2, 3}},
		{name: "partly applied", applied: map[int]bool{1: true}, want: []int{2, 3}},
		{name: "up to date", applied: map[int]bool{1: true, 2: true, 3: true}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []int
			for _, m := range Pending(all, tt.applied) {
				got = append(got, m.Version)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("Pending = %v, want %v", got, tt.want)
			}
		})
	}
}

// The repository's own migrations must always load.
func TestRepositoryMigrations(t *testing.T) {
	tests := []struct {
		name string
		load func() ([]Migration, error)
	}{
		{name: "cassandra", load: func() ([]Migration, error) { return Load(migrations.Cassandra, "cassandra") }},
		{name: "postgres", load: func() ([]Migration, error) { return Load(migrations.Postgres, "postgres") }},
		{name: "clickhouse", load: func() ([]Migration, error) { return Load(migrations.ClickHouse, "clickhouse") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.load()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				t.Fatal("no migrations")
			}
			for i, m := range got {
				if m.Version != i+1 {
					t.Errorf("%s: version %d at position %d, want consecutive versions from 0001", m.Name, m.Version, i)
				}
			}
		})
	}
}
