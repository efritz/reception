package reception

import (
	"github.com/aphistic/sweet"
	. "github.com/onsi/gomega"
)

type UtilSuite struct{}

func (s *UtilSuite) TestMakePath(t sweet.T) {
	Expect(makePath()).To(Equal("/"))
	Expect(makePath("foo")).To(Equal("/foo"))
	Expect(makePath("foo", "bar", "baz")).To(Equal("/foo/bar/baz"))
	Expect(makePath("/foo/", "/bar/", "/baz/")).To(Equal("/foo/bar/baz"))
}
