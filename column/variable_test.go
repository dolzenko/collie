package column

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Variable", func() {
	var subject *Variable
	var _ Column = subject
	var fill = func() {
		Expect(subject.Add([]byte("a"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("ab"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abc"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abcd"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("abc"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("ab"))).NotTo(HaveOccurred())
		Expect(subject.Add([]byte("a"))).NotTo(HaveOccurred())
	}
	var offsets = func() []int64 {
		res := make([]int64, 0)
		buf := make([]byte, 1024)
		n, _ := subject.file.ReadAt(buf, 0)
		for i := 0; i < n; i += 8 {
			res = append(res, int64(binary.BigEndian.Uint64(buf[i:i+8])))
		}
		return res
	}

	JustBeforeEach(func() {
		var err error
		subject, err = OpenVariable(filepath.Join(testDir, "col"))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		subject.Close()
	})

	It("should open new columns", func() {
		Expect(subject.rows).To(Equal(int64(0)))
		Expect(subject.pos).To(Equal(int64(0)))
	})

	It("should add values", func() {
		fill()
		Expect(subject.rows).To(Equal(int64(7)))
		Expect(subject.pos).To(Equal(int64(16)))
		Expect(offsets()).To(Equal([]int64{1, 3, 6, 10, 13, 15, 16}))
	})

	It("should reopen columns", func() {
		fill()
		Expect(subject.Close()).NotTo(HaveOccurred())

		var err error
		subject, err = OpenVariable(filepath.Join(testDir, "col"))
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.rows).To(Equal(int64(7)))
		Expect(subject.pos).To(Equal(int64(16)))
		Expect(offsets()).To(Equal([]int64{1, 3, 6, 10, 13, 15, 16}))
	})

	It("should recover index/data length mismatches", func() {
		fill()
		Expect(subject.Close()).NotTo(HaveOccurred())

		file, err := os.OpenFile(filepath.Join(testDir, "col"), os.O_APPEND|os.O_WRONLY, 0644)
		Expect(err).NotTo(HaveOccurred())
		_, err = file.Write([]byte("foobarbogus"))
		Expect(err).NotTo(HaveOccurred())
		file.Close()

		subject, err = OpenVariable(filepath.Join(testDir, "col"))
		Expect(subject.rows).To(Equal(int64(7)))
		Expect(subject.pos).To(Equal(int64(16)))
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

		val, err = subject.Get(6)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("a")))
		val, err = subject.Get(7)
		Expect(err).To(Equal(ErrNotFound))
		val, err = subject.Get(-1)
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
				err := subject.Add([]byte(fmt.Sprintf("%05d", i)))
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

		var val []byte
		val, err := subject.Get(100)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("00100")))

		val, err = subject.Get(900)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("00900")))

		val, err = subject.Get(1200)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("01200")))
	})

	It("should truncate", func() {
		fill()
		Expect(subject.Len()).To(Equal(int64(7)))
		Expect(subject.pos).To(Equal(int64(16)))

		Expect(subject.Truncate(4)).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(4)))
		Expect(subject.pos).To(Equal(int64(10)))

		Expect(subject.Add([]byte("xxxx"))).NotTo(HaveOccurred())
		Expect(subject.Len()).To(Equal(int64(5)))
		Expect(subject.pos).To(Equal(int64(14)))

		var val []byte
		val, err := subject.Get(3)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("abcd")))
		val, err = subject.Get(4)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("xxxx")))
		val, err = subject.Get(5)
		Expect(err).To(Equal(ErrNotFound))
	})

})

/*************************************************************************
 * BENCHMARKS
 *************************************************************************/

func benchmarkVariable(b *testing.B, max int) {
	dir, err := ioutil.TempDir("", "collie.cols.test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	col, err := OpenVariable(filepath.Join(testDir, "col"))
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	for i := 0; i < b.N; i++ {
		n := rand.Intn(max)
		col.Add([]byte(fmt.Sprintf("%"+strconv.Itoa(n)+"d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = col.Get(int64(rand.Intn(b.N)))
	}
}

func BenchmarkVariableShort(b *testing.B) { benchmarkVariable(b, 32) }
func BenchmarkVariableLong(b *testing.B)  { benchmarkVariable(b, 1024) }
