package vals

type Val string

const (
	// Count should be used as a number of results of a function, if results are countable
	// i.e. slice, array, but not limited to them.
	Count = "count"

	RequestBody  = "request_body"
	ResponseBody = "response_body"

	Result = "result"

	Connection = "connection"
	Params     = "params"
	Query      = "query"
)
