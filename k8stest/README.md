# Kubernetes checks

Run these through GitHub Actions, not on the shared agent host.

- `conformance.yml` runs the full Kubernetes v1.37.0 conformance selection on one kind control plane and two workers. Hydrophone v0.8.0 runs four Ginkgo processes, including serial specs, with no skip or retry. The suite has 120 minutes, the outer command 130 minutes and the job 150 minutes.
- `ci.yml`'s `configmaps` job creates 50,000 named ConfigMaps through the API. It checks every name through paginated API reads and every key through linearizable reads on each store member. It deletes only the namespace, waits at most 15 minutes, then requires an empty API list and no current registry key belonging to that namespace on any member. It does not claim old MVCC versions or WAL bytes have been erased.

Both jobs build the checked-out tigerstore commit and start three members with client and peer TLS. Kind v0.33.0 uses the digest-pinned v1.37.0 node image. The API server has no local etcd. A proof ConfigMap must appear in each external member before either test starts. The three store processes share the runner and disk. Each gets a 1 GiB memory budget, not a cgroup limit. These checks do not prove multi-host durability.

```sh
gh workflow run conformance.yml --repo djosh34/tigerstore --ref <branch>
gh workflow run ci.yml --repo djosh34/tigerstore --ref <branch>
```

For conformance failure triage, dispatch the same commit with `-f store=etcd`. That uses stock etcd v3.7.2 with the same topology. A passing control is evidence to investigate, not proof of a tigerstore cause.

Artifacts contain the commit and binary hashes, configuration without private keys, server and Kubernetes logs, and test results. `conformance/summary.json` requires a zero suite exit and every expected spec exactly once with no selected skips, errors or failures. Missing JUnit fails. Parser tests cover incomplete, duplicate, skipped, interrupted and failed suites. A successful workflow is not a CNCF certification claim.

The 462-name inventory comes from the Kubernetes v1.37.0 full-selection inventory retained on `exp/pre-48-conformance`, file `experiments/pre48/conformance-names-v1.37.0.txt`. That branch compared the names with the upstream conformance inventory and a release-binary dry run. The earlier plan-1 runs were bounded discovery, not full conformance. The external-store setup follows `bench/kind` on `work/146-docs` and the plan-1 kind runs linked from #50.
