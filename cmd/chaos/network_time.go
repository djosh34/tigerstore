package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/djosh34/tigerstore/chaos/proxy"
)

var networkTimeFaults = []string{"leader-incoming", "follower-island", "leader-flap", "peer-slow-leader"}

// networkTime keeps client links healthy while cutting or delaying peer
// traffic. Each transition is recorded; dormant follower-to-follower
// links do not have to carry bytes before the fault can be installed.
func (r *runner) networkTime(ctx context.Context, s step, lead, follower int, rec *record) error {
	var faults []linkFault
	for from := range 3 {
		for to := range 3 {
			if from == to {
				continue
			}
			f := proxy.Fault{Kind: proxy.Cut, Dir: proxy.Forward}
			use := false
			switch s.Fault {
			case "leader-incoming":
				use = to == lead
			case "follower-island":
				use = from == follower || to == follower
				f.Dir = proxy.Both
			case "leader-flap":
				use = from == lead || to == lead
				f.Dir = proxy.Both
			case "peer-slow-leader":
				use = from == lead
				f.Kind, f.Delay = proxy.Delay, 1200*time.Millisecond
			}
			if use {
				faults = append(faults, linkFault{r.c.peers[from][to], f})
			}
		}
	}
	rounds, hold := 1, 12*time.Second
	if s.Fault == "leader-flap" {
		rounds, hold = 24, 250*time.Millisecond
	}
	if s.Fault == "follower-island" {
		hold = 25 * time.Second
	}
	rec.Installed, rec.Healed = true, true
	var errs []error
	for round := 0; round < rounds; round++ {
		base := len(rec.Links)
		for _, lf := range faults {
			st, err := lf.l.Proxy.Set(lf.f)
			rec.Links = append(rec.Links, linkRecord{Link: lf.l.Name, Requested: lf.f, Installed: st})
			if err != nil || st.Fault != lf.f {
				rec.Installed = false
				errs = append(errs, errors.Join(fmt.Errorf("install %s: %+v", lf.l.Name, st.Fault), err))
			}
		}
		errs = append(errs, pause(ctx, hold))
		for i, lf := range faults {
			lr := &rec.Links[base+i]
			lr.BeforeHeal = lf.l.Proxy.Stats()
			rec.Effect = rec.Effect || lr.BeforeHeal.Hits > 0
			st, err := lf.l.Proxy.Set(proxy.Fault{Kind: proxy.None})
			lr.Healed = st
			if err != nil || st.Fault.Kind != proxy.None {
				rec.Healed = false
				errs = append(errs, errors.Join(fmt.Errorf("heal %s: %+v", lf.l.Name, st.Fault), err))
			}
		}
		if round+1 < rounds {
			errs = append(errs, pause(ctx, 250*time.Millisecond))
		}
	}
	rec.HealedAt = r.since()
	if !rec.Effect {
		rec.NoEffect = "no peer traffic hit the topology fault"
	}
	return errors.Join(errs...)
}
