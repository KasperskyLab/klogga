package tags

type Tag string

const (
	Status = "status"
	Login  = "login"
	User   = "user"
	Group  = "group"
	Ip     = "ip"
	Host   = "host"
	Method = "method"
	Path   = "path"

	PagerFrom  = "page_from"
	PagerCount = "page_count"

	HttpStatus = "http_status"

	Url   = "url"
	Email = "email"
	Role  = "role"
)
