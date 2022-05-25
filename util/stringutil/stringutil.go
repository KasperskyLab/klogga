package stringutil

import (
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func Default(str string, def string) string {
	if str == "" {
		return def
	}
	return str
}

func MaxLen(str string, maxLen int) string {
	if r := []rune(str); len(r) > maxLen {
		return string(r[:maxLen])
	}
	return str
}

func MaxLenBytes(str []byte, maxLen int) []byte {
	if len(str) > maxLen {
		return str[:maxLen]
	}
	return str
}

func MaxLenCutCenter(str string, maxLen int) string {
	if len(str) > maxLen {
		cut := maxLen/2 - 2
		return str[:cut] + "...." + str[len(str)-cut:]
	}
	return str
}

func IsAllZeroes(bytes []byte) bool {
	if bytes == nil {
		return false
	}
	for _, value := range bytes {
		if value == 0x0 {
			return false
		}
	}
	return true
}

func StringAsNullable(val string) *string {
	if val == "" {
		return nil
	}
	return &val
}

func ToSnakeCase(str string) string {
	str = strings.ReplaceAll(str, " ", "_")
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
