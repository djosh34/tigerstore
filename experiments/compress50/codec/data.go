package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strings"
)

type record struct {
	Key, Value            []byte
	resource, format, bin string
	train                 bool
}

func load(r io.Reader) ([]record, error) {
	var dump struct {
		KVs []struct {
			Key, Value []byte `json:",omitempty"`
		} `json:"kvs"`
	}
	d := json.NewDecoder(r)
	if err := d.Decode(&dump); err != nil {
		return nil, errors.New("invalid etcdctl JSON or base64")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("unexpected content after dump")
	}
	if len(dump.KVs) == 0 {
		return nil, errors.New("dump contains no kvs")
	}
	out := make([]record, 0, len(dump.KVs))
	seen := make(map[string]bool)
	for _, kv := range dump.KVs {
		if len(kv.Key) == 0 {
			return nil, errors.New("dump contains an empty key")
		}
		if seen[string(kv.Key)] {
			return nil, errors.New("dump contains duplicate keys")
		}
		seen[string(kv.Key)] = true
		h := sha256.Sum256(kv.Key)
		out = append(out, record{kv.Key, kv.Value, resource(kv.Key), format(kv.Value), sizeBin(len(kv.Value)), binary.BigEndian.Uint64(h[:8])%5 == 0})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	return out, nil
}

var (
	knownResources = tokenSet("pods podtemplates minions nodes namespaces secrets configmaps serviceaccounts services endpoints endpointslices deployments replicasets replicationcontrollers daemonsets statefulsets controllerrevisions jobs cronjobs events leases persistentvolumes persistentvolumeclaims storageclasses volumeattachments csinodes csidrivers csistoragecapacities resourcequotas limitranges horizontalpodautoscalers poddisruptionbudgets ingresses ingressclasses networkpolicies roles rolebindings clusterroles clusterrolebindings customresourcedefinitions apiservices priorityclasses runtimeclasses mutatingwebhookconfigurations validatingwebhookconfigurations validatingadmissionpolicies validatingadmissionpolicybindings flowcontrol flowSchemas flowschemas prioritylevelconfigurations certificatesigningrequests resourceclaims resourceclaimtemplates resourceslices deviceclasses")
	knownGroups    = tokenSet("apps batch coordination.k8s.io discovery.k8s.io networking.k8s.io storage.k8s.io rbac.authorization.k8s.io apiextensions.k8s.io apiregistration.k8s.io scheduling.k8s.io node.k8s.io admissionregistration.k8s.io flowcontrol.apiserver.k8s.io certificates.k8s.io resource.k8s.io autoscaling policy")
)

func tokenSet(s string) map[string]bool {
	m := make(map[string]bool)
	for v := range strings.FieldsSeq(s) {
		m[v] = true
	}
	return m
}

func resource(k []byte) string {
	p := strings.Split(string(k), "/")
	if len(p) < 3 || p[1] != "registry" {
		return "other"
	}
	if p[2] == "services" && len(p) > 3 && p[3] == "endpoints" {
		return "endpoints"
	}
	if knownResources[p[2]] {
		return p[2]
	}
	if knownGroups[p[2]] && len(p) > 3 && knownResources[p[3]] {
		return p[3]
	}
	// Never copy an unknown key segment into output.
	return "other"
}

func format(v []byte) string {
	if bytes.HasPrefix(v, []byte{'k', '8', 's', 0}) {
		return "kubernetes_protobuf"
	}
	if json.Valid(v) {
		return "json"
	}
	return "raw_other"
}

func sizeBin(n int) string {
	switch {
	case n == 0:
		return "empty"
	case n < 896:
		return "1_to_895"
	case n <= 1152:
		return "near_1KiB_896_to_1152"
	case n < 9216:
		return "1153_to_9215"
	case n <= 11264:
		return "near_10KiB_9216_to_11264"
	default:
		return "11265_plus"
	}
}

func filter(rs []record, pred func(record) bool) []record {
	var out []record
	for _, r := range rs {
		if pred(r) {
			out = append(out, r)
		}
	}
	return out
}

func synthetic(n int, repeated bool) []record {
	rng := rand.New(rand.NewSource(int64(n)))
	out := make([]record, 256)
	for i := range out {
		v := make([]byte, n)
		if repeated {
			for j := range v {
				v[j] = []byte("abc0123456789")[j%13]
			}
		} else {
			_, _ = rng.Read(v)
		}
		out[i] = record{Key: []byte(fmt.Sprintf("synthetic/%04d", i)), Value: v, resource: "synthetic", format: "raw_other", bin: sizeBin(n)}
	}
	return out
}

type input struct {
	data   []byte
	values int
}

func pack(rs []record, batch int, order string) ([]input, error) {
	items := append([]record(nil), rs...)
	switch order {
	case "single":
		out := make([]input, len(items))
		for i, r := range items {
			out[i] = input{r.Value, 1}
		}
		return out, nil
	case "shuffled":
		rand.New(rand.NewSource(42)).Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	case "resource_sorted":
		sort.Slice(items, func(i, j int) bool {
			if items[i].resource != items[j].resource {
				return items[i].resource < items[j].resource
			}
			return bytes.Compare(items[i].Key, items[j].Key) < 0
		})
	default:
		return nil, errors.New("unknown batch order")
	}
	var out []input
	for start := 0; start < len(items); start += batch {
		end := min(start+batch, len(items))
		var buf []byte
		for _, r := range items[start:end] {
			if uint64(len(r.Value)) > math.MaxUint32 {
				return nil, errors.New("value exceeds u32 framing limit")
			}
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(r.Value)))
			buf = append(buf, r.Value...)
		}
		out = append(out, input{buf, end - start})
	}
	return out, nil
}

func validateFrames(data []byte, count int) error {
	for range count {
		if len(data) < 4 {
			return errors.New("short length frame")
		}
		n := uint64(binary.LittleEndian.Uint32(data))
		data = data[4:]
		if n > uint64(len(data)) {
			return errors.New("short value frame")
		}
		data = data[int(n):]
	}
	if len(data) != 0 {
		return errors.New("trailing frame bytes")
	}
	return nil
}
