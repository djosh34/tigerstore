package wl

import (
	"strconv"

	"github.com/coreos/go-semver/semver"
)

// Downgrades, as etcd v3.7.2's (v3_server.go's Downgrade): VALIDATE
// checks a target, ENABLE starts a downgrade to it through Raft, CANCEL
// ends one. Only the minor version before the cluster's is a target.
// Once a downgrade is on, the cluster version and the storage version
// are the target's, as etcd's leader soon makes them; CANCEL brings them
// back. Checkpoints keep the downgrade with the members.

const (
	serverMinor = 7 // etcd 3.7, which the node serves as

	downgradeValidate = 0
	downgradeEnable   = 1
	downgradeCancel   = 2
)

var (
	errDowngradeFormat     = requestErr(codeInvalidArgument, "etcdserver: wrong downgrade target version format")
	errDowngradeTarget     = requestErr(codeInvalidArgument, "etcdserver: invalid downgrade target version")
	errDowngradeInProgress = requestErr(codeFailedPrecondition, "etcdserver: cluster has a downgrade job in progress")
	errDowngradeAction     = requestErr(codeUnknown, "etcdserver: unknown method")
)

// clusterMinor is the cluster version's minor: the downgrade's target
// while one is on.
func (n *node) clusterMinor() int64 {
	if n.downgrade != 0 {
		return n.downgrade
	}
	return serverMinor
}

// clusterVersion is etcd's cluster version, "3.<minor>".
func (n *node) clusterVersion() string { return "3." + strconv.FormatInt(n.clusterMinor(), 10) }

// storageVersion is etcd's storage version, which follows the cluster's.
func (n *node) storageVersion() string { return n.clusterVersion() + ".0" }

// downgradeTarget is etcd's convertToClusterVersion and its check of the
// target: Major.Minor or a version, the minor before the server's,
// which etcd still compares with while its cluster version is lowered.
func (n *node) downgradeTarget(v string) (int64, error) {
	ver, err := semver.NewVersion(v)
	if err != nil {
		if ver, err = semver.NewVersion(v + ".0"); err != nil {
			return 0, errDowngradeFormat
		}
	}
	if ver.Major != 3 || ver.Minor != serverMinor-1 {
		return 0, errDowngradeTarget
	}
	return ver.Minor, nil
}

type downgradeReq struct {
	action  uint64
	version string
}

func parseDowngrade(m []byte) (downgradeReq, error) {
	var r downgradeReq
	for len(m) > 0 {
		num, _, v, p, rest, err := field(m)
		if err != nil {
			return r, err
		}
		switch num {
		case 1:
			r.action = v
		case 2:
			r.version = string(p)
		}
		m = rest
	}
	return r, nil
}

// checkDowngrade checks a downgrade request against the cluster as it
// is: a downgrade on refuses another and a VALIDATE.
func (n *node) checkDowngrade(r downgradeReq) error {
	switch r.action {
	case downgradeValidate, downgradeEnable:
		if _, err := n.downgradeTarget(r.version); err != nil {
			return err
		}
		if n.downgrade != 0 {
			return errDowngradeInProgress
		}
	case downgradeCancel:
	default:
		return errDowngradeAction
	}
	return nil
}

// downgradeRequest answers a VALIDATE at once and proposes the rest.
func (n *node) downgradeRequest(c *client, s *stream, msg []byte) error {
	r, err := parseDowngrade(msg)
	if err != nil {
		n.reject(c, s, badRequest(err))
		return nil
	}
	if err := n.checkDowngrade(r); err != nil {
		n.reject(c, s, err)
		return nil
	}
	if r.action == downgradeValidate {
		n.replyDowngrade(c, s, n.clusterVersion())
		return nil
	}
	n.addOp(mkHandle(c, s.id), kDowngrade, msg)
	return nil
}

// applyDowngrade starts or ends a downgrade. Its answer names the
// cluster version before it, as etcd's does; a CANCEL's names none.
func (n *node) applyDowngrade(req []byte, c *client, s *stream) error {
	r, err := parseDowngrade(req)
	if err != nil {
		return badRequest(err)
	}
	if err := n.checkDowngrade(r); err != nil {
		return err
	}
	before := n.clusterVersion()
	switch r.action {
	case downgradeEnable:
		n.downgrade, _ = n.downgradeTarget(r.version)
	case downgradeCancel:
		if n.downgrade == 0 {
			break
		}
		n.downgrade, before = 0, ""
	}
	if c != nil {
		n.replyDowngrade(c, s, before)
	}
	return nil
}

func (n *node) replyDowngrade(c *client, s *stream, version string) {
	var w msgW
	n.beginReply(c, s, &w, sizeBytesField(1, n.hdrSize())+sizeStringField(2, version))
	n.writeHdr(&w)
	if version != "" {
		w.bytesField(2, []byte(version))
	}
	n.endReply(c, s, &w)
}

// downgradeInfo is Status's DowngradeInfo: whether a downgrade is on and
// its target version.
func (n *node) downgradeInfo() []byte {
	var b []byte
	if n.downgrade != 0 {
		b = appendVarintField(b, 1, 1)
		b = appendBytesField(b, 2, []byte("3."+strconv.FormatInt(n.downgrade, 10)+".0"))
	}
	return b
}

func sizeStringField(num int, s string) int {
	if s == "" {
		return 0
	}
	return sizeBytesField(num, len(s))
}
