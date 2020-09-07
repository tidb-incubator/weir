package backend

type Instance struct {
	addr string
}

func (i *Instance) Addr() string {
	return i.addr
}
