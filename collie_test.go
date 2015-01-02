package collie

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Row", func() {
	var subject *Row
	var _ Record = subject

	BeforeEach(func() {
		subject = newRow(0, 0)
	})

	It("should set columns", func() {
		subject.SetColumn("a", Value("test1"))
		subject.SetColumn("b", Value("test2"))
		subject.SetColumn("a", Value("test3"))
		Expect(subject.columns).To(HaveLen(2))
		Expect(subject.columns).To(HaveKeyWithValue("a", Value("test3")))
		Expect(subject.columns).To(HaveKeyWithValue("b", Value("test2")))
	})

	It("should add indices", func() {
		subject.AddIndex("a", Value("test1"))
		subject.AddIndex("b", Value("test2"))
		subject.AddIndex("a", Value("test3"))
		Expect(subject.indices).To(HaveLen(2))
		Expect(subject.indices).To(HaveKeyWithValue("a", []Value{Value("test1"), Value("test3")}))
		Expect(subject.indices).To(HaveKeyWithValue("b", []Value{Value("test2")}))
	})

})

/*************************************************************************
 * HELPERS
 *************************************************************************/

var testDir string

type testRecord map[string]Value

func (t testRecord) ValueAt(name string) (Value, error)     { return t[name], nil }
func (t testRecord) IValuesAt(name string) ([]Value, error) { return []Value{t[name]}, nil }

type testRecordBadCol struct{}

func (t testRecordBadCol) ValueAt(name string) (Value, error)     { return nil, io.EOF }
func (t testRecordBadCol) IValuesAt(name string) ([]Value, error) { return []Value{}, nil }

type testRecordBadIndex map[string]Value

func (t testRecordBadIndex) ValueAt(name string) (Value, error) { return t[name], nil }
func (t testRecordBadIndex) IValuesAt(name string) ([]Value, error) {
	if name == "age" {
		return nil, io.EOF
	}
	return []Value{t[name]}, nil
}

/*************************************************************************
 * GINKGO TEST HOOK
 *************************************************************************/

func TestSuite(t *testing.T) {
	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "collie.test")
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		os.RemoveAll(testDir)
	})
	RegisterFailHandler(Fail)
	RunSpecs(t, "collie")
}
