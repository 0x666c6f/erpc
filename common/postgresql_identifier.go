package common

import (
	"fmt"
	"regexp"
	"strings"
)

var postgreSQLIdentifierComponentRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidatePostgreSQLTableIdentifier rejects table identifiers that cannot be
// safely interpolated after pgx identifier quoting.
func ValidatePostgreSQLTableIdentifier(name string) error {
	_, err := PostgreSQLTableIdentifierParts(name)
	return err
}

// PostgreSQLTableIdentifierParts returns one table component or schema/table
// components after validating each component as an unquoted PostgreSQL identifier.
func PostgreSQLTableIdentifierParts(name string) ([]string, error) {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("postgres table identifier %q: too many qualifying parts", name)
	}
	for _, part := range parts {
		if !postgreSQLIdentifierComponentRE.MatchString(part) {
			return nil, fmt.Errorf("postgres table identifier %q: invalid component %q", name, part)
		}
	}
	return parts, nil
}
