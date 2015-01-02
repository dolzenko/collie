package collie

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/bsm/collie/column"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Txn", func() {
	var subject *Txn
	var testDir string

	BeforeEach(func() {
		var err error

		testDir, err = ioutil.TempDir("", "collie.test")
		Expect(err).NotTo(HaveOccurred())

		schema, err := NewSchema([]Column{
			{Name: "first"},
			{Name: "last", Size: 40},
			{Name: "cityID", Size: 4, Index: IndexTypeHash, NoData: true},
			{Name: "age", Size: 1, Index: IndexTypeHash},
			{Name: "active", Size: 1},
		})
		Expect(err).NotTo(HaveOccurred())

		coll, err := OpenCollection(testDir, schema)
		Expect(err).NotTo(HaveOccurred())

		subject = newTxn(coll, 0)
		subject.Add(testRecord{"first": Value("Jane"), "last": Value("Doe"), "age": Value{27}, "cityID": Value{0, 0, 2, 0}, "active": Value{1}})
		subject.Add(testRecord{"first": Value("John"), "last": Value("Doe"), "age": Value{26}, "cityID": Value{0, 0, 2, 99}})
	})

	AfterEach(func() {
		os.RemoveAll(testDir)
	})

	It("should add records", func() {
		n, err := subject.Commit()
		Expect(n).To(Equal(int64(2)))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.c.Offset()).To(Equal(int64(2)))
	})

	It("should add new rows", func() {
		row := subject.New()
		row.SetColumn("first", Value("Jill"))
		row.SetColumn("age", Value{25})
		row.AddIndex("cityID", Value{0, 0, 3, 0})
		row.AddIndex("cityID", Value{0, 0, 3, 1})
		row.AddIndex("age", Value{25})

		n, err := subject.Commit()
		Expect(n).To(Equal(int64(3)))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.c.Offset()).To(Equal(int64(3)))

		offs, err := subject.c.indices["cityID"].Get(Value{0, 0, 3, 0})
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(Equal([]int64{2}))
		offs, err = subject.c.indices["cityID"].Get(Value{0, 0, 3, 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(Equal([]int64{2}))
		offs, err = subject.c.indices["cityID"].Get(Value{0, 0, 3, 2})
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(BeNil())
	})

	Describe("on failures", func() {

		It("should rollback all changes", func() {
			subject.Add(testRecordBadCol{})

			n, err := subject.Commit()
			Expect(n).To(Equal(int64(0)))
			Expect(err).To(Equal(io.EOF))
			Expect(subject.c.Offset()).To(Equal(int64(0)))

			val, err := subject.c.columns["first"].Get(0)
			Expect(val).To(BeNil())
			Expect(err).To(Equal(column.ErrNotFound))
		})

		It("should rollback to previous offset", func() {
			n, err := subject.Commit()
			Expect(n).To(Equal(int64(2)))
			Expect(err).NotTo(HaveOccurred())

			subject.Add(testRecordBadCol{})
			n, err = subject.Commit()
			Expect(n).To(Equal(int64(2)))
			Expect(err).To(Equal(io.EOF))
		})

		It("should undo indices on failures", func() {
			n, err := subject.Commit()
			Expect(n).To(Equal(int64(2)))
			Expect(err).NotTo(HaveOccurred())

			subject.Add(testRecordBadIndex{"age": Value{25}, "cityID": Value{0, 0, 3, 0}})
			n, err = subject.Commit()
			Expect(n).To(Equal(int64(2)))
			Expect(err).To(Equal(io.EOF))

			offs, err := subject.c.indices["age"].Get(Value{25})
			Expect(err).NotTo(HaveOccurred())
			Expect(offs).To(BeNil())

			offs, err = subject.c.indices["cityID"].Get(Value{0, 0, 3, 0})
			Expect(err).NotTo(HaveOccurred())
			Expect(offs).To(BeNil())
		})

	})

})
