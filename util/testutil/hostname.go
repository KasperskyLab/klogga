package testutil

type Hostname struct{}

func NewHostname() Hostname {
	return Hostname{}
}

func (h Hostname) Name() (string, error) {
	return "testmachine", nil
}
