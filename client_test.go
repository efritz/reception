package reception

import (
	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type ClientSuite struct{}

func (s *ClientSuite) TestSerializeMetadata(t sweet.T) {
	service := &Service{
		Address: "localhost",
		Port:    1234,
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	}

	Expect(service.serializeMetadata()).To(MatchJSON(`{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`))
}

func (s *ClientSuite) TestSerializeAttributes(t sweet.T) {
	service := &Service{
		Address: "localhost",
		Port:    1234,
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	}

	Expect(service.serializeAttributes()).To(MatchJSON(`{
		"foo": "bar",
		"baz": "bonk"
	}`))
}

func (s *ClientSuite) TestParseMetadata(t sweet.T) {
	payload := `{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`

	service := &Service{}
	Expect(service.parseMetadata([]byte(payload))).To(BeTrue())
	Expect(service.Address).To(Equal("localhost"))
	Expect(service.Port).To(Equal(1234))
	Expect(service.Attributes).To(Equal(Attributes(map[string]string{
		"foo": "bar",
		"baz": "bonk",
	})))
}

func (s *ClientSuite) TestParseMetadataError(t sweet.T) {
	service := &Service{}
	Expect(service.parseMetadata([]byte(`not json`))).To(BeFalse())
}

func (s *ClientSuite) TestParseAttributes(t sweet.T) {
	payload := `{
		"foo": "bar",
		"baz": "bonk"
	}`

	service := &Service{
		Address: "localhost",
		Port:    1234,
	}

	Expect(service.parseAttributes([]byte(payload))).To(BeTrue())
	Expect(service.Address).To(Equal("localhost"))
	Expect(service.Port).To(Equal(1234))
	Expect(service.Attributes).To(Equal(Attributes(map[string]string{
		"foo": "bar",
		"baz": "bonk",
	})))
}

func (s *ClientSuite) TestParseAttributesError(t sweet.T) {
	service := &Service{}
	Expect(service.parseAttributes([]byte(`not json`))).To(BeFalse())
}
