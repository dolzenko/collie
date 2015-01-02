package column

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Fixed", func() {
	var subject *Fixed
	var _ Column = subject
	var fill = func() {
		Expect(subject.Add([]byte("a"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("ab"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abc"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abcd"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abcde"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abcd"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abc"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("ab"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("a"))).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		var err error
		subject, err = OpenFixed(filepath.Join(testDir, "col"), 4)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should open new columns", func() {
		Expect(subject.rows).To(Equal(int64(0)))
	})

	It("should add values", func() {
		fill()
		Expect(subject.rows).To(Equal(int64(9)))
		Expect(subject.Len()).To(Equal(int64(9)))
	})

	It("should reopen columns", func() {
		fill()
		Expect(subject.Close()).NotTo(HaveOccurred())

		var err error
		subject, err = OpenFixed(filepath.Join(testDir, "col"), 4)
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.rows).To(Equal(int64(9)))
		Expect(subject.Len()).To(Equal(int64(9)))
	})

	It("should read values at index", func() {
		val, err := subject.Get(-1)
		Expect(err).To(Equal(ErrNotFound))
		val, err = subject.Get(0)
		Expect(err).To(Equal(ErrNotFound))
		val, err = subject.Get(10)
		Expect(err).To(Equal(ErrNotFound))

		fill()
		val, err = subject.Get(0)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("a")))
		val, err = subject.Get(2)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("abc")))
		val, err = subject.Get(4)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("abcd")))
		val, err = subject.Get(5)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("abcd")))
		val, err = subject.Get(6)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("abc")))

		val, err = subject.Get(-1)
		Expect(err).To(Equal(ErrNotFound))
		val, err = subject.Get(10)
		Expect(err).To(Equal(ErrNotFound))
	})

	It("should read/write concurrently", func() {
		wait := sync.Mutex{}
		wait.Lock()

		wg := new(sync.WaitGroup)
		wg.Add(2)

		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			wait.Lock()

			for i := 0; i < 2000; i++ {
				err := subject.Add([]byte(fmt.Sprintf("%d", i)))
				Expect(err).NotTo(HaveOccurred())
			}
		}()

		go func() {
			defer wg.Done()
			wait.Unlock()

			for i := 1; i < 2000; i++ {
				_, _ = subject.Get(int64(rand.Intn(i)))
			}
		}()

		wg.Wait()

		val, err := subject.Get(100)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("100")))

		val, err = subject.Get(900)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("900")))

		val, err = subject.Get(1200)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("1200")))
	})

	It("should truncate", func() {
		Expect(subject.Add([]byte("A"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("B"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("C"))).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(3)))

		Expect(subject.Truncate(1)).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(1)))

		Expect(subject.Add([]byte("D"))).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(2)))

		val, err := subject.Get(0)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("A")))
		val, err = subject.Get(1)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("D")))
		_, err = subject.Get(2)
		Expect(err).To(Equal(ErrNotFound))

		Expect(subject.Truncate(0)).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(0)))
		_, err = subject.Get(0)
		Expect(err).To(Equal(ErrNotFound))
	})

})

/*************************************************************************
 * BENCHMARKS
 *************************************************************************/

func benchmarkFixed(b *testing.B, size int) {
	dir, err := ioutil.TempDir("", "collie.cols.test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	col, err := OpenFixed(filepath.Join(dir, "col"), size)
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	for i := 0; i < b.N; i++ {
		if err := col.Add([]byte(fmt.Sprintf("%d", i))); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := rand.Intn(b.N)
		if _, err := col.Get(int64(off)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFixedShort(b *testing.B) { benchmarkFixed(b, 32) }
func BenchmarkFixedLong(b *testing.B)  { benchmarkFixed(b, 1024) }
