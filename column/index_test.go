package column

import (
	"path/filepath"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("HashIndex", func() {
	var subject *HashIndex
	var err error
	var fill = func() {
		Expect(subject.Add([]byte("a"), 1)).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("a"), 2)).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("b"), 3)).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		subject, err = OpenHashIndex(filepath.Join(testDir, "index"))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should add/get values", func() {
		offs, err := subject.Get([]byte("a"))
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(BeNil())

		fill()
		offs, err = subject.Get([]byte("a"))
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(ConsistOf([]int64{1, 2}))

		offs, err = subject.Get([]byte("b"))
		Expect(err).NotTo(HaveOccurred())
		Expect(offs).To(ConsistOf([]int64{3}))
	})

	It("should add values atomically", func() {
		key := []byte("a")

		wg := sync.WaitGroup{}
		for n := 0; n < 10; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for i := n; i < 2000; i += 10 {
					subject.Add(key, int64(i))
				}
			}(n)
		}
		wg.Wait()

		offs, err := subject.Get(key)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(offs)).To(Equal(2000))
	})
})
