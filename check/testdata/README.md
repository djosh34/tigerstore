# Campaign histories

`seed-5068434.json` and `seed-5058843.json` come from simulator campaign
[38928018896](https://github.com/djosh34/tigerstore/actions/runs/38928018896)
on `40e7c0a009a08d6ba80f369dd2e66c9735512dbb`. Both full histories timed out
in the revision-witness search. See #206 and #207.

[Diagnostic run 38935355545](https://github.com/djosh34/tigerstore/actions/runs/38935355545)
regenerated them and kept each successful operation whose header revision
is at most 32. It also kept ambiguous mutations called no later than the
last return of those successful operations. It removed all other operations,
watches, the committed log and the operations' effects. The files therefore
exercise the revision-only checker without using the simulator's log as an
oracle.

Keep these fixtures when the simulator changes. A change to fault handling
can generate a different history for the same seed and hide the checker
regression.

`seed-77148.json` comes from simulator campaign
[39013428691](https://github.com/djosh34/tigerstore/actions/runs/39013428691)
on `b27d2b6b`, where the revision witness timed out (#237). It was cut the
same way, keeping the successful operations whose header revision is at
most 150 and the ambiguous mutations called no later than their last
return. A lease's ambiguous expiry deletes its keys, so the search tried it
in every gap of a long outage, although an acked put still used the lease
after them.

`seed-59113215.json` comes from the full `ci` set on PR #238 at `a5c0fbff`
([39029055912](https://github.com/djosh34/tigerstore/actions/runs/39029055912),
`sim (3)`), where the revision witness timed out. It was cut the same way
at header revision 70. A lease's ambiguous expiry, called when the lease
was granted, came first among the candidates of every gap and fitted one
that a delete wrote, so the search tried every order of the gaps after it.
