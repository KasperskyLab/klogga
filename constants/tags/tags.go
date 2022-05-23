package tags

type Tag string

const (
	Status = "status"
	Login  = "login"
	User   = "user"
	Group  = "group"
	IP     = "ip"
	Host   = "host"
	Method = "method"
	Path   = "path"

	PagerFrom  = "page_from"
	PagerCount = "page_count"

	HTTPStatus = "http_status"

	URL   = "url"
	Email = "email"
	Role  = "role"
)
