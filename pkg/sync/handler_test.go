package sync

import (
	. "gopkg.in/check.v1"
	"net/http"
)

type TestSuite struct{}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestRemoveReferer(c *C) {
	req, err := http.NewRequest("HEAD", "https://foo.bar", nil)
	c.Assert(err, IsNil)
	req.Header.Set("Referer", "https://foo.bar")
	req.Header.Set("Foo", "foo")
	removeReferer(req)
	c.Assert(req.Referer(), Equals, "")
	c.Assert(req.Header, HasLen, 1)
}
