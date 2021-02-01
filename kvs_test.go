package txshardv2

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKvsUnique(t *testing.T) {
	s := newKvsUnique(2)
	s.put([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "100",
		},
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "103",
		},
	})

	expected := []CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "100",
		},
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
	}
	assert.Equal(t, expected, s.next())

	s.completed([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "100",
		},
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
	})

	expected = []CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "103",
		},
	}
	assert.Equal(t, expected, s.next())
	s.completed([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "103",
		},
	})

	expected = nil
	assert.Equal(t, expected, s.next())

	s.put([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "1030",
		},
		{
			Type:  EventTypePut,
			Key:   "key04",
			Value: "104",
		},
	})

	expected = []CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "1030",
		},
		{
			Type:  EventTypePut,
			Key:   "key04",
			Value: "104",
		},
	}
	assert.Equal(t, expected, s.next())
	s.next()
}

func TestKvsUnique_Deduplicate_WaitList_To_Input(t *testing.T) {
	s := newKvsUnique(2)
	s.put([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "100",
		},
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "103",
		},
	})

	s.put([]CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "1010",
		},
	})

	s.put([]CASKeyValue{})

	expected := []CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key02",
			Value: "102",
		},
		{
			Type:  EventTypePut,
			Key:   "key03",
			Value: "103",
		},
	}
	assert.Equal(t, expected, s.next())

	expected = []CASKeyValue{
		{
			Type:  EventTypePut,
			Key:   "key01",
			Value: "1010",
		},
	}
	assert.Equal(t, expected, s.next())
}
