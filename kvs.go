package txshardv2

type kvsUnique struct {
	limit        int
	waitingKvs   []CASKeyValue
	completedKvs map[string]CASKeyValue
}

func newKvsUnique(limit int) *kvsUnique {
	return &kvsUnique{
		limit:        limit,
		completedKvs: make(map[string]CASKeyValue),
	}
}

func (u *kvsUnique) put(kvs []CASKeyValue) {
	if len(kvs) == 0 {
		return
	}

	inputMap := make(map[string]CASKeyValue)
	for _, kv := range kvs {
		inputMap[kv.Key] = kv
	}

	var filtered []CASKeyValue
	for _, kv := range u.waitingKvs {
		_, existed := inputMap[kv.Key]
		if existed {
			continue
		}
		filtered = append(filtered, kv)
	}

	u.waitingKvs = filtered

	for _, kv := range kvs {
		value, ok := u.completedKvs[kv.Key]
		if ok && value == kv {
			continue
		}

		u.waitingKvs = append(u.waitingKvs, kv)
	}
}

func (u *kvsUnique) next() []CASKeyValue {
	n := len(u.waitingKvs)
	if n == 0 {
		return nil
	}

	if n > u.limit {
		n = u.limit
	} else {
		result := make([]CASKeyValue, n)
		copy(result, u.waitingKvs)
		u.waitingKvs = u.waitingKvs[:0]
		return result
	}

	result := make([]CASKeyValue, n)
	copy(result, u.waitingKvs)

	remain := len(u.waitingKvs) - n
	copy(u.waitingKvs, u.waitingKvs[n:])
	u.waitingKvs = u.waitingKvs[:remain]

	return result
}

func (u *kvsUnique) completed(kvs []CASKeyValue) {
	for _, kv := range kvs {
		u.completedKvs[kv.Key] = kv
	}
}
