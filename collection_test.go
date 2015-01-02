package collie

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Collection", func() {
	var subject *Collection
	var schema *Schema

	BeforeEach(func() {
		var err error
		schema, err = NewSchema([]Column{
			{Name: "first"},
			{Name: "last", Size: 40},
			{Name: "accountIds", Size: 4, Index: IndexTypeHash, NoData: true},
			{Name: "age", Size: 1, Index: IndexTypeHash},
			{Name: "active", Size: 1},
		})
		Expect(err).NotTo(HaveOccurred())

		subject, err = OpenCollection(testDir, schema)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should register types", func() {
		Expect(subject.columns).To(HaveLen(4))
		Expect(subject.columns).To(HaveKey("first"))
		Expect(subject.columns).To(HaveKey("last"))
		Expect(subject.columns).To(HaveKey("age"))
		Expect(subject.columns).To(HaveKey("active"))
		Expect(subject.indices).To(HaveLen(2))
		Expect(subject.indices).To(HaveKey("accountIds"))
		Expect(subject.indices).To(HaveKey("age"))
	})

	Describe("input/output", func() {

		BeforeEach(func() {
			txn := subject.Begin(2)
			txn.Add(testRecord{"first": []byte("Jane"), "last": []byte("Doe"), "age": []byte{27}, "accountIds": []byte{0, 0, 2, 0}, "active": []byte{1}})
			txn.Add(testRecord{"first": []byte("John"), "last": []byte("Doe"), "age": []byte{26}, "accountIds": []byte{0, 0, 2, 99}})

			n, err := txn.Commit()
			Expect(n).To(Equal(int64(2)))
			Expect(err).NotTo(HaveOccurred())
		})

		It("should add records", func() {
			Expect(subject.offset).To(Equal(int64(2)))
		})

		It("should re-open databases", func() {
			err := subject.Close()
			Expect(err).NotTo(HaveOccurred())
			subject, err = OpenCollection(testDir, schema)
			Expect(err).NotTo(HaveOccurred())
			Expect(subject.offset).To(Equal(int64(2)))
		})

		It("should get values at offset", func() {
			first, err := subject.Value("first", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(first)).To(Equal("Jane"))

			last, err := subject.Value("last", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(last)).To(Equal("Doe"))

			_, err = subject.Value("last", 2)
			Expect(err).To(Equal(ErrNotFound))

			_, err = subject.Value("lost", 1)
			Expect(err).To(Equal(ErrColumnNotFound))
		})

		It("should query index offsets", func() {
			offsets, err := subject.Offsets("age", []byte{26})
			Expect(err).NotTo(HaveOccurred())
			Expect(offsets).To(Equal([]int64{1}))

			offsets, err = subject.Offsets("age", []byte{27})
			Expect(err).NotTo(HaveOccurred())
			Expect(offsets).To(Equal([]int64{0}))

			offsets, err = subject.Offsets("age", []byte{127})
			Expect(err).NotTo(HaveOccurred())
			Expect(offsets).To(BeEmpty())

			offsets, err = subject.Offsets("age", []byte{0, 0, 0, 26})
			Expect(err).NotTo(HaveOccurred())
			Expect(offsets).To(BeEmpty())

			_, err = subject.Offsets("first", []byte("Jane"))
			Expect(err).To(Equal(ErrColumnNotFound))
		})

	})

})
