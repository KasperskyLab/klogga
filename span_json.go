package klogga

import (
	"encoding/json"
	"github.com/KasperskyLab/klogga/util/errs"
)

func (s *Span) MarshalJSON() ([]byte, error) {
	if s == nil {
		return nil, nil
	}

	jsonMap := map[string]any{}

	for k, v := range s.Tags() {
		jsonMap[k] = v
	}
	for k, v := range s.Vals() {
		jsonMap[k] = v
	}

	jsonMap["id"] = s.ID()
	jsonMap["parent_id"] = s.ParentID()
	jsonMap["trace_id"] = s.TraceID()
	jsonMap["started"] = s.StartedTs().Format(TimestampLayout)
	jsonMap["duration"] = s.Duration()
	jsonMap["level"] = s.level.String()
	jsonMap["component"] = s.component
	jsonMap["package_class"] = s.PackageClass()
	jsonMap["name"] = s.Name()
	jsonMap["error"] = errs.Append(s.Errs(), s.DeferErrs())
	jsonMap["warn"] = s.Warns()
	jsonMap["tags"] = s.Tags()
	jsonMap["vals"] = s.Vals()

	return json.Marshal(jsonMap)
}

// Json DEPRECATED for compatibility with earlier versions
func (s *Span) Json() ([]byte, error) {
	return s.MarshalJSON()
}
