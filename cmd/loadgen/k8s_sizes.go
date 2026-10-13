package main

import (
	"math"
	"math/rand/v2"
)

// Kubernetes 1.37 dump from #50 comment 6134373626, inventory.csv at
// exp/pre-50-compress:evidence/compress50/run-38319776822/inventory.csv.
// Only all,<resource>,*,<size_bin> rows. Mean is value_bytes / count.
// Resource and bin probabilities use object counts, not byte shares.
type sizeBin struct {
	resource, bin   string
	count, min, max int
	mean            float64
}

var k8sSizes = []sizeBin{
	{"apiservices", "1_to_895", 18, 827, 894, 15789.0 / 18},
	{"apiservices", "near_1KiB_896_to_1152", 4, 898, 908, 3616.0 / 4},
	{"certificatesigningrequests", "1153_to_9215", 1, 2400, 2400, 2400.0 / 1},
	{"clusterrolebindings", "1_to_895", 63, 341, 757, 39645.0 / 63},
	{"clusterrolebindings", "near_1KiB_896_to_1152", 1, 955, 955, 955.0 / 1},
	{"clusterroles", "1153_to_9215", 8, 1194, 3811, 18704.0 / 8},
	{"clusterroles", "1_to_895", 65, 242, 895, 42305.0 / 65},
	{"clusterroles", "near_1KiB_896_to_1152", 4, 904, 1056, 3871.0 / 4},
	{"configmaps", "11265_plus", 24, 33365, 33365, 800760.0 / 24},
	{"configmaps", "1153_to_9215", 202, 1463, 8786, 539084.0 / 202},
	{"configmaps", "1_to_895", 2, 264, 630, 894.0 / 2},
	{"configmaps", "near_10KiB_9216_to_11264", 120, 10834, 10834, 1300080.0 / 120},
	{"configmaps", "near_1KiB_896_to_1152", 37, 946, 1106, 40762.0 / 37},
	{"controllerrevisions", "1153_to_9215", 2, 2031, 2388, 4419.0 / 2},
	{"csinodes", "1_to_895", 1, 682, 682, 682.0 / 1},
	{"daemonsets", "1153_to_9215", 2, 2829, 3516, 6345.0 / 2},
	{"deployments", "1153_to_9215", 122, 3361, 4466, 414004.0 / 122},
	{"endpoints", "1_to_895", 121, 348, 879, 105707.0 / 121},
	{"endpoints", "near_1KiB_896_to_1152", 1, 977, 977, 977.0 / 1},
	{"endpointslices", "1153_to_9215", 265, 1201, 1876, 372650.0 / 265},
	{"endpointslices", "1_to_895", 97, 406, 893, 86134.0 / 97},
	{"events", "1_to_895", 3083, 490, 890, 2160927.0 / 3083},
	{"events", "near_1KiB_896_to_1152", 720, 898, 899, 647040.0 / 720},
	{"flowschemas", "near_1KiB_896_to_1152", 11, 941, 1149, 11403.0 / 11},
	{"leases", "1_to_895", 244, 430, 743, 161383.0 / 244},
	{"minions", "1153_to_9215", 1, 4106, 4106, 4106.0 / 1},
	{"namespaces", "1_to_895", 17, 290, 559, 5231.0 / 17},
	{"other", "1153_to_9215", 1, 1473, 1473, 1473.0 / 1},
	{"other", "1_to_895", 125, 63, 536, 65556.0 / 125},
	{"other", "near_1KiB_896_to_1152", 1, 951, 951, 951.0 / 1},
	{"pods", "1153_to_9215", 129, 4455, 7576, 655067.0 / 129},
	{"priorityclasses", "1_to_895", 2, 375, 407, 782.0 / 2},
	{"prioritylevelconfigurations", "1_to_895", 8, 503, 645, 4912.0 / 8},
	{"replicasets", "1153_to_9215", 430, 2950, 4103, 1386485.0 / 430},
	{"rolebindings", "1_to_895", 11, 382, 810, 6565.0 / 11},
	{"rolebindings", "near_1KiB_896_to_1152", 1, 978, 978, 978.0 / 1},
	{"roles", "1_to_895", 12, 256, 753, 6006.0 / 12},
	{"secrets", "1153_to_9215", 48, 1593, 4665, 150192.0 / 48},
	{"secrets", "1_to_895", 73, 617, 825, 53033.0 / 73},
	{"serviceaccounts", "1_to_895", 61, 108, 529, 7930.0 / 61},
	{"services", "1_to_895", 121, 638, 714, 86120.0 / 121},
	{"services", "near_1KiB_896_to_1152", 1, 1143, 1143, 1143.0 / 1},
	{"storageclasses", "1_to_895", 1, 841, 841, 841.0 / 1},
}

// The inventory has no finer within-bin histogram. Model each nonconstant
// bin with a scaled beta distribution: alpha=8*p, beta=8*(1-p), where
// p=(mean-min)/(max-min). Its support uses the exact observed min/max and
// its expectation is the exact bin mean. Stochastic integer rounding keeps
// that expectation. This is a smooth fitted model, not an empirical draw.
func (b sizeBin) draw(r *rand.Rand) int {
	if b.min == b.max {
		return b.min
	}
	p := (b.mean - float64(b.min)) / float64(b.max-b.min)
	if p <= 0 {
		return b.min
	}
	if p >= 1 {
		return b.max
	}
	a, c := gammaDraw(r, 8*p), gammaDraw(r, 8*(1-p))
	x := float64(b.min) + float64(b.max-b.min)*a/(a+c)
	n := int(x)
	if r.Float64() < x-float64(n) {
		n++
	}
	return n
}

// Marsaglia-Tsang gamma sampler, with the shape-raising identity below 1.
func gammaDraw(r *rand.Rand, shape float64) float64 {
	if shape < 1 {
		return gammaDraw(r, shape+1) * math.Pow(r.Float64(), 1/shape)
	}
	d := shape - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		x := r.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := r.Float64()
		if u < 1-0.0331*x*x*x*x || math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

func drawBin(r *rand.Rand, resource string) int {
	total := 0
	for _, b := range k8sSizes {
		if resource == "" || resource == b.resource {
			total += b.count
		}
	}
	n := r.IntN(total)
	for i, b := range k8sSizes {
		if resource == "" || resource == b.resource {
			n -= b.count
			if n < 0 {
				return i
			}
		}
	}
	return 0
}

func k8sResources() []string {
	var out []string
	for _, b := range k8sSizes {
		if len(out) == 0 || out[len(out)-1] != b.resource {
			out = append(out, b.resource)
		}
	}
	return out
}
