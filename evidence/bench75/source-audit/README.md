# Source-count evidence

`audit.md` gives the scope and conditional counts. `verified-sites.md` preserves 160 checked source sites. `library-evidence` preserves the pinned module source used for the copy census. The production files remain available at the exact commits in `refs.json`.

To rerun the citation checks, extract those commits beside this README. No Go or benchmark execution is needed:

```sh
here=$PWD/evidence/bench75/source-audit
mkdir -p "$here/main" "$here/pr75-latest" "$here/pr76"
git archive e1fd889c345bc3c06d8100eb6d635c88ad6967cd | tar -x -C "$here/main"
git archive 8fcdd021aea23eca9b9d012f56faee13be39f1fc | tar -x -C "$here/pr75-latest"
git archive b3e6f6237c6d6a7fb1ad6018efffe8cee6bf4c56 | tar -x -C "$here/pr76"
python3 "$here/verify-sites.py"
```

The manifest is the helper's original evidence manifest. It includes hashes for the locally extracted source and review files as well as the archived citation and module evidence. It does not claim that every temporary review file is committed here.
