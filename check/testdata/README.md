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
