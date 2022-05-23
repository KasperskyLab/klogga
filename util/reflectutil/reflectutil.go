package reflectutil

import (
	"reflect"
	"runtime"
	"strings"
)

// GetPackageClassFunc parses package (last entry in the path),
// class(==receiver type) and func for the call one level up
func GetPackageClassFunc() (string, string, string) {
	pc := make([]uintptr, 10)
	runtime.Callers(2, pc)

	fnc := runtime.FuncForPC(pc[1])
	fullName := fnc.Name()

	// remove path to package
	i := strings.LastIndex(fullName, "/")
	if i >= 0 && i+1 < len(fullName) {
		fullName = fullName[i+1:]
	}

	split := strings.Split(fullName, ".")
	if len(split) >= 3 {
		return split[0], cleanReceiver(split[1]), split[2]
	}

	if len(split) == 2 {
		return split[0], "", split[1]
	}
	return fullName, "", ""
}

func cleanReceiver(c string) string {
	c = strings.Trim(c, "()")
	return strings.TrimPrefix(c, "*")
}

// IsNil universal check for nil
// https://medium.com/@glucn/golang-an-interface-holding-a-nil-value-is-not-nil-bb151f472cc7
func IsNil(i interface{}) bool {
	return i == nil || (reflect.ValueOf(i).Kind() == reflect.Ptr && reflect.ValueOf(i).IsNil())
}
