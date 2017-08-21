package reception

import (
	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type ClientSuite struct{}

func (s *ClientSuite) TestSerialize(t sweet.T) {
	service := &Service{
		Address: "localhost",
		Port:    1234,
		Attributes: map[string]string{
			"foo": "bar",
			"baz": "bonk",
		},
	}

	Expect(service.SerializeMetadata()).To(MatchJSON(`{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`))
}

func (s *ClientSuite) TestParse(t sweet.T) {
	payload := `{
		"address": "localhost",
		"port": 1234,
		"attributes": {
			"foo": "bar",
			"baz": "bonk"
		}
	}`

	service := &Service{}
	Expect(parseMetadata(service, []byte(payload))).To(BeTrue())
	Expect(service.Address).To(Equal("localhost"))
	Expect(service.Port).To(Equal(1234))
	Expect(service.Attributes).To(Equal(Attributes(map[string]string{
		"foo": "bar",
		"baz": "bonk",
	})))
}

func (s *ClientSuite) TestParseError(t sweet.T) {
	Expect(parseMetadata(&Service{}, []byte(`not json`))).To(BeFalse())
}
