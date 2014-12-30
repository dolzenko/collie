package column

import (
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*************************************************************************
 * GINKGO TEST HOOK
 *************************************************************************/

var testDir string

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "collie.column.test")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(testDir)
	})
	RunSpecs(t, "collie/column")
}
