package collie

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Schema", func() {

	It("should create new schemata", func() {
		schema, err := NewSchema([]Column{
			{Name: "first", Size: 30},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(schema.columns).To(HaveLen(1))
	})

	It("should reject bad columns", func() {
		_, err := NewSchema([]Column{
			{Name: "bad name"},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal(`collie: invalid column name 'bad name'`))
	})

	It("should reject duplicate columns", func() {
		_, err := NewSchema([]Column{
			{Name: "first", Size: 30},
			{Name: "first", Size: 20},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal("collie: duplicate column 'first'"))
	})

})
