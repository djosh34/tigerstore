// Reduce etcdctl JSON inside the kind node. Never print keys or values.
package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

var resources = tokens("pods podtemplates minions nodes namespaces secrets configmaps serviceaccounts services endpoints endpointslices deployments replicasets replicationcontrollers daemonsets statefulsets controllerrevisions jobs cronjobs events leases persistentvolumes persistentvolumeclaims storageclasses volumeattachments csinodes csidrivers csistoragecapacities resourcequotas limitranges horizontalpodautoscalers poddisruptionbudgets ingresses ingressclasses networkpolicies roles rolebindings clusterroles clusterrolebindings customresourcedefinitions apiservices priorityclasses runtimeclasses mutatingwebhookconfigurations validatingwebhookconfigurations validatingadmissionpolicies validatingadmissionpolicybindings flowcontrol flowSchemas flowschemas prioritylevelconfigurations certificatesigningrequests resourceclaims resourceclaimtemplates resourceslices deviceclasses")
var groups = tokens("apps batch coordination.k8s.io discovery.k8s.io networking.k8s.io storage.k8s.io rbac.authorization.k8s.io apiextensions.k8s.io apiregistration.k8s.io scheduling.k8s.io node.k8s.io admissionregistration.k8s.io flowcontrol.apiserver.k8s.io certificates.k8s.io resource.k8s.io autoscaling policy")

func tokens(s string) map[string]bool {
	m := make(map[string]bool)
	for _, token := range strings.Fields(s) {
		m[token] = true
	}
	return m
}

// Match the compression pre-work's key-prefix classifier, not value Kind fields.
// Unknown segments never reach the output.
func resource(key []byte) string {
	p := strings.Split(string(key), "/")
	if len(p) < 3 || p[1] != "registry" {
		return "other"
	}
	if p[2] == "services" && len(p) > 3 && p[3] == "endpoints" {
		return "endpoints"
	}
	if resources[p[2]] {
		return p[2]
	}
	if groups[p[2]] && len(p) > 3 && resources[p[3]] {
		return p[3]
	}
	return "other"
}

func export(r io.Reader, out io.Writer) error {
	var dump struct {
		KVs []struct {
			Key, Value []byte
		} `json:"kvs"`
		Count int64 `json:"count"`
		More  bool  `json:"more"`
	}
	d := json.NewDecoder(r)
	if d.Decode(&dump) != nil {
		return errors.New("invalid dump")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("trailing dump data")
	}
	if dump.More || len(dump.KVs) == 0 || dump.Count != int64(len(dump.KVs)) {
		return errors.New("incomplete dump")
	}
	seen := make(map[string]bool)
	for _, kv := range dump.KVs {
		if len(kv.Key) == 0 || seen[string(kv.Key)] {
			return errors.New("empty or duplicate key")
		}
		seen[string(kv.Key)] = true
	}
	w := csv.NewWriter(out)
	if err := w.Write([]string{"resource", "value_bytes"}); err != nil {
		return errors.New("output failed")
	}
	for _, kv := range dump.KVs {
		if err := w.Write([]string{resource(kv.Key), strconv.Itoa(len(kv.Value))}); err != nil {
			return errors.New("output failed")
		}
	}
	w.Flush()
	if w.Error() != nil {
		return errors.New("output failed")
	}
	return nil
}

func main() {
	if err := export(os.Stdin, os.Stdout); err != nil {
		// Do not include parser errors, keys, values or input fragments.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
