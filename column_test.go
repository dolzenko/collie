package collie

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Column", func() {

	It("should validate", func() {
		err := (&Column{}).Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal(`collie: invalid column name ''`))

		err = (&Column{Name: "in valid"}).Validate()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(Equal(`collie: invalid column name 'in valid'`))

		Expect((&Column{Name: "x"}).Validate()).NotTo(HaveOccurred())
	})

})
