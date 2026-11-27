package wl

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestResidentOneShotBuffersStayWithinP(t *testing.T) {
	for _, mib := range []int64{83, 137, 191} {
		t.Run(strconv.FormatInt(mib, 10), func(t *testing.T) {
			c, _, st, log := replyRoomClient(t)
			n := c.node
			st.setBudget(mib<<20, 0)
			n.sizes = sizeFor(mib << 20)
			n.cursorFetch = true
			n.connOut = 64 << 10
			for i := range 100 {
				putValue(t, st, log, fmt.Sprintf("/small/%03d", i), strings.Repeat("v", 16<<10))
			}
			clients := []*client{c}
			count := int(n.sizes.clientInBufs() / int64(clientInBuf+clientPtBuf+slotBytes))
			for i := 1; i < count; i++ {
				other := newClient(&handoff{fd: -1, wr: c.wr, rd: c.rd}, n)
				other.takeBufs()
				other.idx = i
				other.gen = 1
				n.socks = append(n.socks, other)
				n.gens = append(n.gens, 1)
				clients = append(clients, other)
			}
			if n.cpool > n.sizes.clientInBufs() {
				t.Fatal("fixture bypassed input admission")
			}
			msg := appendBytesField(appendBytesField(nil, 1, []byte("/small/")), 2, []byte("/small0"))
			for _, cl := range clients {
				t.Cleanup(cl.dropAll)
				s := cl.newStream(3, kRange)
				cl.flow.conn = 0
				s.win = 0
				serveRoomRange(t, cl, s, msg)
				if used := n.stageCredits.used + n.stageOther(0); used > n.sizes.clientPool {
					t.Fatalf("resident buffers exceeded P: %d > %d", used, n.sizes.clientPool)
				}
				if len(n.fills) != 0 || n.fillBook != nil || len(n.bigs) != 0 || st.loads != 0 {
					t.Fatal("one-shot resident reply allocated a source cursor or entered large/cold path")
				}
			}
			if n.stageCredits.used == 0 || len(n.roomWait) == 0 {
				t.Fatal("fixture did not exercise funded copies AND output-budget backpressure")
			}
			t.Logf("B=%dMiB clients=%d P=%d funded=%d other=%d waiting=%d", mib, len(clients), n.sizes.clientPool, n.stageCredits.used, n.stageOther(0), len(n.roomWait))
		})
	}
}
