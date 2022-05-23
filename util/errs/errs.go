package errs

import (
	mr "github.com/hashicorp/go-multierror"
)

// Append concatenates errors into one, creating a clean message if there is only one error
// ignores all nils, wherever they are
func Append(err error, errs ...error) error {
	var cleanErrs []error
	for _, errAdd := range errs {
		if errAdd != nil {
			cleanErrs = append(cleanErrs, errAdd)
		}
	}
	if len(cleanErrs) == 0 {
		return err
	} else if err == nil && len(cleanErrs) == 1 {
		return cleanErrs[0]
	}
	return mr.Append(err, cleanErrs...)
}
