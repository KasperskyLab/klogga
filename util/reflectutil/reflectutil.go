package reflectutil

import (
	"net/url"
	"reflect"
	"runtime"
	"strings"

	"github.com/hashicorp/go-version"
)

const pathSeparator = string("/")

// GetPackageClassFunc parses package (last entry in the path),
// class(==receiver type) and func for the call one level up
func GetPackageClassFunc(skip int) (string, string, string) {
	pc, _, _, _ := runtime.Caller(skip)

	fnc := runtime.FuncForPC(pc)
	// We have something like "path.to/my/pkg.MyFunction". If the function is
	// a closure, it is something like, "path.to/my/pkg.MyFunction.func1".
	fullName := fnc.Name()

	// remove path to package
	// Everything up to the first "." after the last "/" is the package name.
	// Everything after the "." is the full function name.
	shortName, rest := base(fullName, pathSeparator)
	split := strings.Split(shortName, ".")
	if len(split) >= 3 {
		return ParsePackageName(rest + pathSeparator + split[0]), cleanReceiver(split[1]), split[2]
	}

	if len(split) == 2 {
		return ParsePackageName(rest + pathSeparator + split[0]), "", split[1]
	}
	return ParsePackageName(fullName), "", ""
}

// ParsePackageName determines the package name based on the path to it. Recognizes and ignores library versioning
// specified via /v* or .v*. Not able to determine the real name of the package, it relies on the folder in which
// the package is located, ignoring the folder for versioning(like /v2) or its suffix(like .v2). If the package name is
// packageA and it is in the folder packageB, then the package will be recognized as packageB.
// Returns the query decoded package name
// implemented in response for issue https://github.com/KasperskyLab/klogga/issues/8
func ParsePackageName(fullPath string) string {
	pkgName, parentDir := base(fullPath, pathSeparator)
	if pkgName == "" {
		return pkgName
	}
	// Package names are URL-encoded to avoid ambiguity in the case where the
	// package name contains ".git". Otherwise, "foo/bar.git.MyFunction" would
	// mean that "git" is the top-level function and "MyFunction" is embedded
	// inside it.
	if unescaped, err := url.QueryUnescape(pkgName); err == nil {
		pkgName = unescaped
	}

	// https://go.dev/ref/mod#major-version-suffixes
	// Starting with major version 2, module paths must have a major version suffix like /v2 that matches the major
	// version. For example, if a module has the path example.com/mod at v1.0.0, it must have the path example.com/mod/v2
	// at version v2.0.0.
	//
	// As a special case, modules paths starting with gopkg.in/ must always have a major version suffix, even at v0 and
	// v1. The suffix must start with a dot rather than a slash (for example, gopkg.in/yaml.v2).

	var foundPkgName string

	// detect /v2 version, like google-api-go-client/blob/main/slides/v1
	checkVersion := pkgName
	if checkVersion[0] == 'v' {
		checkVersion = checkVersion[1:]
	}
	if _, err := version.NewVersion(checkVersion); err == nil {
		foundPkgName, _ = base(parentDir, pathSeparator)
	} else {
		// detect .v2 version, like gopkg.in/yaml.v2
		right, left := base(pkgName, ".v")
		if right != "" {
			if _, err := version.NewVersion(right); err == nil {
				foundPkgName = left
			}
		}
	}
	if foundPkgName == "" {
		foundPkgName = pkgName
	}

	// It has been a common practice in the past to name go package repositories either with go- prefix
	// (like go-bindata or go-iter,…), so remove this prefix if meet
	// https://groups.google.com/g/golang-nuts/c/WMVf2Acq6JQ?pli=1
	// Also this prefix can be in suffix, like in aws/smithy-go or edsrzf/mmap-go
	// Also this prefix can be golang-(hashicorp/golang-lru) or go.(satori/go.uuid)
	return trimPrefix(strings.TrimSuffix(foundPkgName, "-go"), "go-", "golang-", "go.")
}

// trimPrefix removes one of the prefixes, does not remove more than one prefix at a time
func trimPrefix(str string, prefixes ...string) string {
	originLen := len(str)
	for _, prefix := range prefixes {
		str = strings.TrimPrefix(str, prefix)
		if len(str) != originLen {
			return str
		}
	}
	return str
}

func base(fullName, separator string) (name string, dir string) {
	i := strings.LastIndex(fullName, separator)
	if i < 0 || i >= len(fullName)-1 {
		return fullName, ""
	}
	return fullName[i+len(separator):], fullName[:i]
}

func cleanReceiver(c string) string {
	c = strings.Trim(c, "()")
	return strings.TrimPrefix(c, "*")
}

// IsNil universal check for nil
// https://medium.com/@glucn/golang-an-interface-holding-a-nil-value-is-not-nil-bb151f472cc7
func IsNil(i interface{}) bool {
	if reflect.TypeOf(i) == nil {
		return true
	}
	switch reflect.ValueOf(i).Kind() {
	// this set of types was taken from reflect.Value.IsNil()
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.UnsafePointer, reflect.Slice, reflect.Interface:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
