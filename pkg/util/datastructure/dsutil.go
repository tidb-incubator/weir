package datastructure

func StringSliceToSet(ss []string) map[string]struct{} {
	sset := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		sset[s] = struct{}{}
	}
	return sset
}
