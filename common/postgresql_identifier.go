package common

import (
	"fmt"
	"strings"
	"unicode"
)

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
		if !isPostgreSQLIdentifierComponent(part) {
			return nil, fmt.Errorf("postgres table identifier %q: invalid component %q", name, part)
		}
	}
	return parts, nil
}

func isPostgreSQLIdentifierComponent(name string) bool {
	for i, r := range name {
		if i == 0 {
			if r != '_' && !unicode.IsLetter(r) {
				return false
			}
			continue
		}
		if r != '_' && r != '$' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return name != ""
}
