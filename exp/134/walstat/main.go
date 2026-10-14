// Question (#134): how many bytes does one Put or one CAS Txn cost in
// tigerstore's log on main, split into what each byte is for?
//
// Throwaway. Two commands:
//
//	walstat count <data dir>...    parse every *.wal under the dirs, print a JSON breakdown
//	walstat sample <dev> <ms> <pids> <out.csv>   sample /proc/diskstats and node CPU until killed
//
// count reads the segment files byte by byte with the format from
// internal/wal/wal.go and internal/core/codec.go on main 76952348.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

type stats struct {
	Files, SegHeaderBytes, FileBytes          int64
	EntryRecords, HardStateRecords            int64
	RecHeaderBytes                            int64 // 9 bytes per record: len, crc, type
	EntryHeaderBytes                          int64 // 17 bytes per entry record: term, index, entry type
	HardStateBytes                            int64 // 24 bytes per hard state record
	EmptyEntries, ConfEntries                 int64
	BatchCountBytes                           int64 // uvarint write count per entry
	Writes                                    map[string]int64
	MetaBytes                                 map[string]int64 // origin, boot, id, kind, length per write
	WireBytes                                 map[string]int64 // the client's request bytes
	KeyBytes, ValueBytes                      map[string]int64 // key and value payload inside the wire bytes
	TornTail                                  int64
	OriginBytes, BootBytes, IDBytes, LenBytes map[string]int64
}

var kinds = map[byte]string{1: "put", 2: "delete", 3: "txn", 4: "compact", 5: "grant", 6: "revoke", 7: "renew", 8: "expiry"}

func uv(b []byte) (uint64, int) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		panic("bad uvarint")
	}
	return v, n
}

// walk returns the key and value bytes in a protobuf message, recursing into
// nested messages the way a Put or a Txn nests them: PutRequest key 1 value 2;
// TxnRequest compare 1 success 2 failure 3; RequestOp range 1 put 2;
// Compare key 3; RangeRequest key 1.
func payload(b []byte, kind string) (key, val int64) {
	var rec func(b []byte, path string)
	rec = func(b []byte, path string) {
		for len(b) > 0 {
			num, typ, n := protowire.ConsumeTag(b)
			if n < 0 {
				return
			}
			b = b[n:]
			if typ != protowire.BytesType {
				m := protowire.ConsumeFieldValue(num, typ, b)
				if m < 0 {
					return
				}
				b = b[m:]
				continue
			}
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return
			}
			b = b[m:]
			p := path + "/" + strconv.Itoa(int(num))
			switch p {
			case "put/1", "txn/2/2/1", "txn/3/1/1", "txn/1/3":
				key += int64(len(v))
			case "put/2", "txn/2/2/2":
				val += int64(len(v))
			case "txn/1", "txn/2", "txn/3", "txn/2/2", "txn/3/1", "txn/2/1", "txn/3/2":
				rec(v, p)
			}
		}
	}
	rec(b, kind)
	return
}

func count(dirs []string) {
	s := stats{Writes: map[string]int64{}, MetaBytes: map[string]int64{}, WireBytes: map[string]int64{},
		KeyBytes: map[string]int64{}, ValueBytes: map[string]int64{},
		OriginBytes: map[string]int64{}, BootBytes: map[string]int64{}, IDBytes: map[string]int64{}, LenBytes: map[string]int64{}}
	for _, d := range dirs {
		filepath.WalkDir(d, func(p string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() || !strings.HasSuffix(p, ".wal") {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil {
				panic(err)
			}
			s.Files++
			s.FileBytes += int64(len(data))
			s.SegHeaderBytes += 20
			off := 20
			for off < len(data) {
				if len(data)-off < 9 {
					s.TornTail += int64(len(data) - off)
					break
				}
				n := int(binary.LittleEndian.Uint32(data[off:]))
				end := off + 9 + n
				if end > len(data) || n == 0 {
					s.TornTail += int64(len(data) - off)
					break
				}
				typ := data[off+8]
				body := data[off+9 : end]
				s.RecHeaderBytes += 9
				switch typ {
				case 2:
					s.HardStateRecords++
					s.HardStateBytes += int64(len(body))
				case 1:
					s.EntryRecords++
					s.EntryHeaderBytes += 17
					etype := body[16]
					ed := body[17:]
					if etype != 0 {
						s.ConfEntries++
						s.MetaBytes["conf"] += int64(len(ed))
						break
					}
					if len(ed) == 0 {
						s.EmptyEntries++
						break
					}
					cnt, k := uv(ed)
					s.BatchCountBytes += int64(k)
					ed = ed[k:]
					for range cnt {
						start := len(ed)
						_, a := uv(ed)
						ed = ed[a:]
						_, b := uv(ed)
						ed = ed[b:]
						_, c := uv(ed)
						ed = ed[c:]
						kind := kinds[ed[0]]
						ed = ed[1:]
						size, l := uv(ed)
						ed = ed[l:]
						s.OriginBytes[kind] += int64(a)
						s.BootBytes[kind] += int64(b)
						s.IDBytes[kind] += int64(c)
						s.LenBytes[kind] += int64(l)
						s.MetaBytes[kind] += int64(start - len(ed))
						s.Writes[kind]++
						s.WireBytes[kind] += int64(size)
						kb, vb := payload(ed[:size], kind)
						s.KeyBytes[kind] += kb
						s.ValueBytes[kind] += vb
						ed = ed[size:]
					}
					if len(ed) != 0 {
						panic("batch has trailing bytes")
					}
				default:
					panic(fmt.Sprintf("record type %d", typ))
				}
				off = end
			}
			return nil
		})
	}
	json.NewEncoder(os.Stdout).Encode(s)
}

// sample prints one line per tick: unix ns, the device's diskstats fields,
// then utime+stime ticks of each pid.
func sample(dev string, ms int, pids []string, out string) {
	f, err := os.Create(out)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	t := time.NewTicker(time.Duration(ms) * time.Millisecond)
	for {
		b, _ := os.ReadFile("/proc/diskstats")
		line := ""
		for _, l := range strings.Split(string(b), "\n") {
			fs := strings.Fields(l)
			if len(fs) > 3 && fs[2] == dev {
				line = strings.Join(fs[3:], " ")
			}
		}
		fmt.Fprintf(w, "%d %s", time.Now().UnixNano(), line)
		for _, p := range pids {
			st, err := os.ReadFile("/proc/" + p + "/stat")
			ticks := int64(-1)
			if err == nil {
				fs := strings.Fields(string(st)[strings.LastIndexByte(string(st), ')')+1:])
				u, _ := strconv.ParseInt(fs[11], 10, 64)
				sy, _ := strconv.ParseInt(fs[12], 10, 64)
				ticks = u + sy
			}
			fmt.Fprintf(w, " %d", ticks)
		}
		fmt.Fprintln(w)
		w.Flush()
		<-t.C
	}
}

func main() {
	switch os.Args[1] {
	case "count":
		count(os.Args[2:])
	case "sample":
		ms, _ := strconv.Atoi(os.Args[3])
		sample(os.Args[2], ms, strings.Split(os.Args[4], ","), os.Args[5])
	}
}
