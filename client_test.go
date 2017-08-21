package reception

import (
	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type ClientSuite struct{}

func (s *ClientSuite) TestSerialize(t sweet.T) {
	service := &Service{Metadata: map[string]string{
		"foo": "bar",
		"baz": "bonk",
	}}

	Expect(service.SerializedMetadata()).To(MatchJSON(`{
		"foo": "bar",
		"baz": "bonk"
	}`))
}

func (s *ClientSuite) TestParse(t sweet.T) {
	metadata, ok := parseMetadata([]byte(`{"foo": "bar", "baz": "bonk"}`))
	Expect(ok).To(BeTrue())
	Expect(metadata).To(Equal(Metadata(map[string]string{
		"foo": "bar",
		"baz": "bonk",
	})))
}

func (s *ClientSuite) TestParseError(t sweet.T) {
	_, ok := parseMetadata([]byte(`not json`))
	Expect(ok).To(BeFalse())
}
