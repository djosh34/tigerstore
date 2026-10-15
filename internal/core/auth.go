package core

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/authpb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Auth follows etcd v3.7.2's auth store (server/auth) and its checks
// (server/etcdserver/apply/auth.go, v3_server.go and api/v3rpc). Users,
// roles, whether auth is enabled and the auth revision are replicated
// state, held in checkpoints; every auth request goes through the log,
// as in etcd. A client is a user by a token from Authenticate, or by its
// certificate's common name with --client-cert-auth. Writes carry the
// user and the auth revision they were taken with, and their permissions
// are checked when they apply, on every replica alike; reads and the
// other calls are checked when the node serves them.

const (
	rootUser = "root"
	rootRole = "root"
)

// errNoPasswordUser is etcd's answer to a password for a user without
// one, which etcd's gRPC layer has no code for.
var errNoPasswordUser = status.Error(codes.Unknown, "auth: authentication failed, password was given for no password user")

// authState is the replicated part of etcd's auth store, and each user's
// merged permissions, rebuilt on every change.
type authState struct {
	enabled  bool
	revision uint64
	users    map[string]*authpb.User
	roles    map[string]*authpb.Role
	perms    map[string]*userPerms
}

func newAuthState() *authState {
	return &authState{revision: 1, users: map[string]*authpb.User{}, roles: map[string]*authpb.Role{}, perms: map[string]*userPerms{}}
}

// authInfo is who a request comes from: a user and the auth revision its
// identity was taken at. The zero authInfo is etcd's empty AuthInfo.
type authInfo struct {
	user     string
	revision uint64
}

// span is a range of keys [begin, end); a nil end is open.
type span struct{ begin, end []byte }

// userPerms are a user's read and write permissions, merged into sorted
// spans that do not touch.
type userPerms struct{ read, write []span }

// rebuild recomputes every user's merged permissions, as etcd's
// refreshRangePermCache does after each change.
func (a *authState) rebuild() {
	a.perms = map[string]*userPerms{}
	for name, u := range a.users {
		var read, write []span
		for _, rn := range u.Roles {
			r := a.roles[rn]
			if r == nil {
				continue
			}
			for _, p := range r.KeyPermission {
				s := span{begin: p.Key}
				switch {
				case len(p.RangeEnd) == 0:
					s.end = append(slices.Clone(p.Key), 0)
				case len(p.RangeEnd) == 1 && p.RangeEnd[0] == 0:
				default:
					s.end = p.RangeEnd
				}
				switch p.PermType {
				case authpb.Permission_READWRITE:
					read, write = append(read, s), append(write, s)
				case authpb.Permission_READ:
					read = append(read, s)
				case authpb.Permission_WRITE:
					write = append(write, s)
				}
			}
		}
		a.perms[name] = &userPerms{read: merge(read), write: merge(write)}
	}
}

// merge sorts spans and joins those that overlap or touch.
func merge(ss []span) []span {
	slices.SortFunc(ss, func(x, y span) int { return bytes.Compare(x.begin, y.begin) })
	var out []span
	for _, s := range ss {
		if n := len(out); n > 0 && (out[n-1].end == nil || bytes.Compare(s.begin, out[n-1].end) <= 0) {
			if out[n-1].end != nil && (s.end == nil || bytes.Compare(s.end, out[n-1].end) > 0) {
				out[n-1].end = s.end
			}
			continue
		}
		out = append(out, s)
	}
	return out
}

// covers reports whether the spans hold key, or the whole range [key,
// end) with Range's meaning of end, as etcd's interval tree does.
func covers(ss []span, key, end []byte) bool {
	i := spanAt(ss, key)
	if i < 0 {
		return false
	}
	s := ss[i]
	if s.end != nil && bytes.Compare(key, s.end) >= 0 {
		return false
	}
	switch {
	case len(end) == 0:
		return true
	case len(end) == 1 && end[0] == 0:
		return s.end == nil
	}
	return s.end == nil || bytes.Compare(end, s.end) <= 0
}

// spanAt is the index of the last span that starts at or before key, or
// -1.
func spanAt(ss []span, key []byte) int {
	i, _ := slices.BinarySearchFunc(ss, key, func(s span, k []byte) int {
		if bytes.Compare(s.begin, k) <= 0 {
			return -1
		}
		return 1
	})
	return i - 1
}

func hasRoot(u *authpb.User) bool { return slices.Contains(u.Roles, rootRole) }

// permitted is etcd's isOpPermitted.
func (a *authState) permitted(ai authInfo, key, end []byte, write bool) error {
	if !a.enabled {
		return nil
	}
	if ai.revision == 0 {
		return rpctypes.ErrGRPCUserEmpty
	}
	if ai.revision < a.revision {
		return rpctypes.ErrGRPCAuthOldRevision
	}
	u := a.users[ai.user]
	if u == nil {
		return rpctypes.ErrGRPCPermissionDenied
	}
	if hasRoot(u) {
		return nil
	}
	p := a.perms[ai.user]
	spans := p.read
	if write {
		spans = p.write
	}
	if covers(spans, key, end) {
		return nil
	}
	return rpctypes.ErrGRPCPermissionDenied
}

// admin is etcd's IsAdminPermitted.
func (a *authState) admin(ai *authInfo) error {
	if !a.enabled {
		return nil
	}
	if ai == nil || ai.user == "" {
		return rpctypes.ErrGRPCUserEmpty
	}
	u := a.users[ai.user]
	if u == nil {
		return rpctypes.ErrGRPCUserNotFound
	}
	if !hasRoot(u) {
		return rpctypes.ErrGRPCPermissionDenied
	}
	return nil
}

// changed counts an auth change: a new revision and new merged
// permissions.
func (a *authState) changed() {
	a.revision++
	a.rebuild()
}

// isValidPermissionRange is etcd's.
func isValidPermissionRange(key, end []byte) bool {
	switch {
	case len(key) == 0:
		return false
	case len(end) == 0, bytes.Compare(key, end) < 0:
		return true
	}
	return len(end) == 1 && end[0] == 0
}

// applyAuth runs an auth request from the log, with the identity its
// origin took it with, as etcd's authApplierV3 and the auth store do.
// index is the entry's index, which a simple token ends with.
func (n *Node) applyAuth(r *pb.InternalRaftRequest, index uint64) (proto.Message, error) {
	a := n.state.auth
	ai := authInfo{user: r.GetHeader().GetUsername(), revision: r.GetHeader().GetAuthRevision()}
	if needsAdmin(r) {
		if err := a.admin(&ai); err != nil {
			return nil, err
		}
	}
	h := n.authHeader()
	switch {
	case r.AuthEnable != nil:
		if a.enabled {
			return &pb.AuthEnableResponse{Header: h}, nil
		}
		u := a.users[rootUser]
		if u == nil {
			return nil, rpctypes.ErrGRPCRootUserNotExist
		}
		if !hasRoot(u) {
			return nil, rpctypes.ErrGRPCRootRoleNotExist
		}
		a.enabled = true
		a.rebuild()
		return &pb.AuthEnableResponse{Header: h}, nil
	case r.AuthDisable != nil:
		if a.enabled {
			a.enabled = false
			a.changed()
			n.tokens = map[string]*simpleToken{}
		}
		return &pb.AuthDisableResponse{Header: h}, nil
	case r.AuthStatus != nil:
		return &pb.AuthStatusResponse{Header: h, Enabled: a.enabled, AuthRevision: a.revision}, nil
	case r.Authenticate != nil:
		return n.authenticate(r.Authenticate, index, h)
	case r.AuthUserAdd != nil:
		return a.userAdd(r.AuthUserAdd, h)
	case r.AuthUserDelete != nil:
		name := r.AuthUserDelete.Name
		if a.enabled && name == rootUser {
			return nil, rpctypes.ErrGRPCInvalidAuthMgmt
		}
		if a.users[name] == nil {
			return nil, rpctypes.ErrGRPCUserNotFound
		}
		delete(a.users, name)
		a.changed()
		n.invalidateTokens(name)
		return &pb.AuthUserDeleteResponse{Header: h}, nil
	case r.AuthUserChangePassword != nil:
		c := r.AuthUserChangePassword
		u := a.users[c.Name]
		if u == nil {
			return nil, rpctypes.ErrGRPCUserNotFound
		}
		next := &authpb.User{Name: u.Name, Roles: u.Roles, Options: u.Options}
		if u.Options == nil || !u.Options.NoPassword {
			if next.Password = hashedPassword(c.HashedPassword); next.Password == nil {
				return nil, errNoPasswordUser
			}
		}
		a.users[c.Name] = next
		a.changed()
		n.invalidateTokens(c.Name)
		return &pb.AuthUserChangePasswordResponse{Header: h}, nil
	case r.AuthUserGrantRole != nil:
		g := r.AuthUserGrantRole
		u := a.users[g.User]
		if u == nil {
			return nil, rpctypes.ErrGRPCUserNotFound
		}
		if g.Role != rootRole && a.roles[g.Role] == nil {
			return nil, rpctypes.ErrGRPCRoleNotFound
		}
		if i, found := slices.BinarySearch(u.Roles, g.Role); !found {
			u = proto.CloneOf(u)
			u.Roles = slices.Insert(u.Roles, i, g.Role)
			a.users[g.User] = u
			a.changed()
		}
		return &pb.AuthUserGrantRoleResponse{Header: h}, nil
	case r.AuthUserRevokeRole != nil:
		rv := r.AuthUserRevokeRole
		if a.enabled && rv.Name == rootUser && rv.Role == rootRole {
			return nil, rpctypes.ErrGRPCInvalidAuthMgmt
		}
		u := a.users[rv.Name]
		if u == nil {
			return nil, rpctypes.ErrGRPCUserNotFound
		}
		i, found := slices.BinarySearch(u.Roles, rv.Role)
		if !found {
			return nil, rpctypes.ErrGRPCRoleNotGranted
		}
		u = proto.CloneOf(u)
		u.Roles = slices.Delete(u.Roles, i, i+1)
		a.users[rv.Name] = u
		a.changed()
		return &pb.AuthUserRevokeRoleResponse{Header: h}, nil
	case r.AuthUserGet != nil:
		if err := a.admin(&ai); err != nil && r.AuthUserGet.Name != ai.user {
			return nil, err
		}
		u := a.users[r.AuthUserGet.Name]
		if u == nil {
			return nil, rpctypes.ErrGRPCUserNotFound
		}
		return &pb.AuthUserGetResponse{Header: h, Roles: slices.Clone(u.Roles)}, nil
	case r.AuthUserList != nil:
		resp := &pb.AuthUserListResponse{Header: h}
		resp.Users = sortedKeys(a.users)
		return resp, nil
	case r.AuthRoleAdd != nil:
		name := r.AuthRoleAdd.Name
		if name == "" {
			return nil, rpctypes.ErrGRPCRoleEmpty
		}
		if a.roles[name] != nil {
			return nil, rpctypes.ErrGRPCRoleAlreadyExist
		}
		a.roles[name] = &authpb.Role{Name: []byte(name)}
		// etcd counts the change but rebuilds nothing: no user has the
		// role yet.
		a.revision++
		return &pb.AuthRoleAddResponse{Header: h}, nil
	case r.AuthRoleGet != nil:
		role := r.AuthRoleGet.Role
		if err := a.admin(&ai); err != nil && !a.hasRole(ai.user, role) {
			return nil, err
		}
		ro := a.roles[role]
		if ro == nil {
			return nil, rpctypes.ErrGRPCRoleNotFound
		}
		resp := &pb.AuthRoleGetResponse{Header: h}
		if role == rootRole {
			resp.Perm = []*authpb.Permission{{PermType: authpb.Permission_READWRITE, Key: []byte{}, RangeEnd: []byte{0}}}
		} else {
			for _, p := range ro.KeyPermission {
				resp.Perm = append(resp.Perm, proto.CloneOf(p))
			}
		}
		return resp, nil
	case r.AuthRoleList != nil:
		resp := &pb.AuthRoleListResponse{Header: h}
		resp.Roles = sortedKeys(a.roles)
		return resp, nil
	case r.AuthRoleDelete != nil:
		name := r.AuthRoleDelete.Role
		if a.enabled && name == rootRole {
			return nil, rpctypes.ErrGRPCInvalidAuthMgmt
		}
		if a.roles[name] == nil {
			return nil, rpctypes.ErrGRPCRoleNotFound
		}
		delete(a.roles, name)
		for un, u := range a.users {
			if i, found := slices.BinarySearch(u.Roles, name); found {
				u = proto.CloneOf(u)
				u.Roles = slices.Delete(u.Roles, i, i+1)
				a.users[un] = u
			}
		}
		a.changed()
		return &pb.AuthRoleDeleteResponse{Header: h}, nil
	case r.AuthRoleGrantPermission != nil:
		g := r.AuthRoleGrantPermission
		if g.Perm == nil {
			return nil, rpctypes.ErrGRPCPermissionNotGiven
		}
		if !isValidPermissionRange(g.Perm.Key, g.Perm.RangeEnd) {
			return nil, rpctypes.ErrGRPCInvalidAuthMgmt
		}
		ro := a.roles[g.Name]
		if ro == nil {
			return nil, rpctypes.ErrGRPCRoleNotFound
		}
		ro = proto.CloneOf(ro)
		i := slices.IndexFunc(ro.KeyPermission, func(p *authpb.Permission) bool {
			return bytes.Compare(p.Key, g.Perm.Key) >= 0
		})
		if i >= 0 && bytes.Equal(ro.KeyPermission[i].Key, g.Perm.Key) && bytes.Equal(ro.KeyPermission[i].RangeEnd, g.Perm.RangeEnd) {
			ro.KeyPermission[i].PermType = g.Perm.PermType
		} else {
			ro.KeyPermission = append(ro.KeyPermission, &authpb.Permission{Key: g.Perm.Key, RangeEnd: g.Perm.RangeEnd, PermType: g.Perm.PermType})
			slices.SortStableFunc(ro.KeyPermission, func(x, y *authpb.Permission) int { return bytes.Compare(x.Key, y.Key) })
		}
		a.roles[g.Name] = ro
		a.changed()
		return &pb.AuthRoleGrantPermissionResponse{Header: h}, nil
	case r.AuthRoleRevokePermission != nil:
		rv := r.AuthRoleRevokePermission
		ro := a.roles[rv.Role]
		if ro == nil {
			return nil, rpctypes.ErrGRPCRoleNotFound
		}
		keep := &authpb.Role{Name: ro.Name}
		for _, p := range ro.KeyPermission {
			if !bytes.Equal(p.Key, rv.Key) || !bytes.Equal(p.RangeEnd, rv.RangeEnd) {
				keep.KeyPermission = append(keep.KeyPermission, p)
			}
		}
		if len(keep.KeyPermission) == len(ro.KeyPermission) {
			return nil, rpctypes.ErrGRPCPermissionNotGranted
		}
		a.roles[rv.Role] = keep
		a.changed()
		return &pb.AuthRoleRevokePermissionResponse{Header: h}, nil
	}
	return nil, fmt.Errorf("%w: an auth request with nothing in it", errBadBatch)
}

// userAdd is etcd's UserAdd. The server hashed the password.
func (a *authState) userAdd(r *pb.AuthUserAddRequest, h *pb.ResponseHeader) (proto.Message, error) {
	if r.Name == "" {
		return nil, rpctypes.ErrGRPCUserEmpty
	}
	if a.users[r.Name] != nil {
		return nil, rpctypes.ErrGRPCUserAlreadyExist
	}
	opts := r.Options
	if opts == nil {
		opts = &authpb.UserAddOptions{}
	}
	u := &authpb.User{Name: []byte(r.Name), Options: opts}
	if !opts.NoPassword {
		if u.Password = hashedPassword(r.HashedPassword); u.Password == nil {
			return nil, errNoPasswordUser
		}
	}
	a.users[r.Name] = u
	a.changed()
	return &pb.AuthUserAddResponse{Header: h}, nil
}

func (a *authState) hasRole(user, role string) bool {
	u := a.users[user]
	return u != nil && slices.Contains(u.Roles, role)
}

// needsAdmin is etcd's needAdminPermission.
func needsAdmin(r *pb.InternalRaftRequest) bool {
	return r.AuthEnable != nil || r.AuthDisable != nil || r.AuthUserAdd != nil || r.AuthUserDelete != nil ||
		r.AuthUserChangePassword != nil || r.AuthUserGrantRole != nil || r.AuthUserRevokeRole != nil ||
		r.AuthRoleAdd != nil || r.AuthRoleGrantPermission != nil || r.AuthRoleRevokePermission != nil ||
		r.AuthRoleDelete != nil || r.AuthUserList != nil || r.AuthRoleList != nil
}

func (n *Node) authHeader() *pb.ResponseHeader {
	h := header(n.kv.Rev())
	n.fillHeader(h)
	return h
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Simple tokens follow etcd's tokenSimple: the node that takes an
// Authenticate picks a random prefix, and every node, when the entry
// applies, makes "<prefix>.<entry index>" a token of the user. A token
// that is not used for AuthTokenTicks goes. Tokens are not replicated
// state: a restart forgets them, as etcd's does.

// simpleToken is a token's user and the tick it expires at.
type simpleToken struct {
	user    string
	expires int64
}

// authenticate applies an InternalAuthenticateRequest: the server checked
// the password against the hash it read.
func (n *Node) authenticate(r *pb.InternalAuthenticateRequest, index uint64, h *pb.ResponseHeader) (proto.Message, error) {
	a := n.state.auth
	if !a.enabled {
		return nil, rpctypes.ErrGRPCAuthNotEnabled
	}
	u := a.users[r.Name]
	if u == nil || (u.Options != nil && u.Options.NoPassword) {
		return nil, rpctypes.ErrGRPCAuthFailed
	}
	resp := &pb.AuthenticateResponse{Header: h}
	if r.SimpleToken != "" {
		resp.Token = r.SimpleToken + "." + strconv.FormatUint(index, 10)
		n.tokens[resp.Token] = &simpleToken{user: r.Name, expires: n.now + int64(n.tokenTicks())}
	}
	return resp, nil
}

// tokenTicks is how long an unused simple token lives.
func (n *Node) tokenTicks() int { return cmp.Or(n.cfg.AuthTokenTicks, 300*n.cfg.TicksPerSecond) }

func (n *Node) invalidateTokens(user string) {
	for t, st := range n.tokens {
		if st.user == user {
			delete(n.tokens, t)
		}
	}
}

// expireTokens drops the simple tokens that were not used in time.
func (n *Node) expireTokens() {
	for t, st := range n.tokens {
		if st.expires <= n.now {
			delete(n.tokens, t)
		}
	}
}

// errTokenNotApplied marks a simple token whose Authenticate entry this
// node has not applied yet; the request waits for it, as etcd's does.
var errTokenNotApplied = errors.New("core: the token's entry is not applied yet")

// identify is etcd's AuthInfoFromCtx: the user of a request from its
// token, or with client certificate authentication its certificate's
// common name. With auth disabled, only a certificate names a user, for
// the size limit. ok is false without an identity.
func (n *Node) identify(c Caller) (ai authInfo, ok bool, err error) {
	a := n.state.auth
	if c.root {
		return authInfo{user: rootUser, revision: a.revision}, true, nil
	}
	if a.enabled && c.Token != "" {
		if c.Verified {
			// A token the server checked itself (JWT).
			if c.User == "" {
				return authInfo{}, false, rpctypes.ErrGRPCInvalidAuthToken
			}
			return authInfo{user: c.User, revision: c.Revision}, true, nil
		}
		st, found := n.tokens[c.Token]
		if !found {
			_, index, cut := strings.Cut(c.Token, ".")
			if i, err := strconv.ParseUint(index, 10, 64); cut && err == nil && i > n.applied {
				return authInfo{}, false, errTokenNotApplied
			}
			return authInfo{}, false, rpctypes.ErrGRPCInvalidAuthToken
		}
		st.expires = n.now + int64(n.tokenTicks())
		return authInfo{user: st.user, revision: a.revision}, true, nil
	}
	if c.name != "" {
		return authInfo{user: c.name, revision: a.revision}, true, nil
	}
	return authInfo{}, false, nil
}

// identityExtra is what the identity adds to a write's marshaled
// request.
func identityExtra(c Caller, ai authInfo, ok bool) int {
	switch {
	case !ok:
		return 0
	case ai.user == c.name && ai.revision == 1:
		return c.extra
	}
	return identityBytes(ai.user, ai.revision)
}

// tokenWait is a request whose token's Authenticate entry this node has
// not applied yet.
type tokenWait struct {
	req      Request
	deadline int64
}

// retryTokenWaits submits again the requests that waited for their
// token, once its entry applied; one that waited past its deadline fails
// with ErrGRPCInvalidAuthToken, as etcd's does.
func (n *Node) retryTokenWaits() {
	waits := n.tokenWaits
	n.tokenWaits = nil
	for _, w := range waits {
		if w.deadline <= n.now {
			n.reply(w.req.ID, rpctypes.ErrGRPCInvalidAuthToken)
			continue
		}
		n.submit(w.req, &pending{})
	}
}

// authorize is the check etcd's gRPC layer and server make before they
// run a request, other than a range, which the node checks when it reads
// (readAllowed), and writes, which it checks when they apply.
func (n *Node) authorize(op proto.Message, ai authInfo, ok bool) error {
	a := n.state.auth
	if !a.enabled {
		return nil
	}
	var who *authInfo
	if ok {
		who = &ai
	}
	switch op := op.(type) {
	case *pb.CompactionRequest, *pb.HashRequest, *pb.HashKVRequest, *pb.MoveLeaderRequest, *pb.DefragmentRequest, *pb.DowngradeRequest,
		*pb.MemberAddRequest, *pb.MemberRemoveRequest, *pb.MemberUpdateRequest, *pb.MemberPromoteRequest:
		return a.admin(who)
	case *pb.AlarmRequest:
		if isAlarmChange(op) || op.Action != pb.AlarmRequest_GET {
			return a.admin(who)
		}
		return requireIdentity(ok)
	case *pb.StatusRequest:
		return requireIdentity(ok)
	case *pb.LeaseKeepAliveRequest:
		if !ok {
			return rpctypes.ErrGRPCUserEmpty
		}
		return n.leaseKeysAllowed(ai, op.ID, true)
	case *pb.LeaseTimeToLiveRequest:
		if !ok {
			return rpctypes.ErrGRPCUserEmpty
		}
		return n.leaseKeysAllowed(ai, op.ID, false)
	case *pb.LeaseLeasesRequest:
		if !ok {
			return rpctypes.ErrGRPCUserEmpty
		}
		for _, l := range n.state.sortedLeases() {
			if err := n.leaseKeysAllowed(ai, l.id, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func requireIdentity(ok bool) error {
	if !ok {
		return rpctypes.ErrGRPCUserEmpty
	}
	return nil
}

// leaseKeysAllowed is etcd's checkLeaseRenew (write) and
// checkLeaseTimeToLive (read): an admin may, others need the permission
// on each key of the lease.
func (n *Node) leaseKeysAllowed(ai authInfo, id int64, write bool) error {
	a := n.state.auth
	if a.admin(&ai) == nil {
		return nil
	}
	l := n.state.leases[id]
	if l == nil {
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(l.keys)) {
		if err := a.permitted(ai, []byte(k), nil, write); err != nil {
			return err
		}
	}
	return nil
}

// readAllowed checks a range or a read-only transaction as etcd's
// doSerialize does, with the identity the request came with.
func (n *Node) readAllowed(p *pending) error {
	a := n.state.auth
	if !a.enabled {
		return nil
	}
	ai := p.ai
	switch op := p.req.Op.(type) {
	case *pb.RangeRequest:
		return a.permitted(ai, op.Key, op.RangeEnd, false)
	case *pb.TxnRequest:
		st := n.state
		st.who = ai
		return st.txnAllowed(op)
	}
	return nil
}

// credentials answers the server's read of a user's password hash before
// it checks a password, as etcd's CheckPassword does, with the auth
// revision in Reply.Rev.
func (n *Node) credentials(r *pb.AuthenticateRequest) (proto.Message, error) {
	a := n.state.auth
	if !a.enabled {
		return nil, rpctypes.ErrGRPCAuthNotEnabled
	}
	u := a.users[r.Name]
	if u == nil {
		return nil, rpctypes.ErrGRPCAuthFailed
	}
	if u.Options != nil && u.Options.NoPassword {
		return nil, errNoPasswordUser
	}
	return proto.CloneOf(u), nil
}

// hashedPassword is the bcrypt hash a request carries, base64 encoded:
// the server hashed the password before the request went into the log,
// as etcd's does. nil means the hash does not decode.
func hashedPassword(hash string) []byte {
	b, err := base64.StdEncoding.DecodeString(hash)
	if err != nil {
		return nil
	}
	return b
}

// Writes are checked as they apply, with the identity they carry
// (State.who), as etcd's authApplierV3 checks them.

// putAllowed is etcd's checkPutAuth.
func (st *State) putAllowed(key []byte, lease int64, prevKV bool) error {
	a := st.auth
	if !a.enabled {
		return nil
	}
	if err := a.permitted(st.who, key, nil, true); err != nil {
		return err
	}
	if err := st.leaseAllowed(lease); err != nil {
		return err
	}
	if prevKV {
		return a.permitted(st.who, key, nil, false)
	}
	return nil
}

// leaseAllowed is etcd's checkLeasePuts: a lease's keys must all be
// writable, unless the user is an admin.
func (st *State) leaseAllowed(id int64) error {
	l := st.leases[id]
	if l == nil || st.auth.admin(&st.who) == nil {
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(l.keys)) {
		if err := st.auth.permitted(st.who, []byte(k), nil, true); err != nil {
			return err
		}
	}
	return nil
}

// writeAllowed checks a write other than a put in wire form.
func (st *State) writeAllowed(op proto.Message) error {
	if !st.auth.enabled {
		return nil
	}
	switch op := op.(type) {
	case *pb.PutRequest:
		return st.putAllowed(op.Key, op.Lease, op.PrevKv)
	case *pb.DeleteRangeRequest:
		return st.deleteAllowed(op)
	case *pb.TxnRequest:
		return st.txnAllowed(op)
	case *pb.LeaseRevokeRequest:
		return st.leaseAllowed(op.ID)
	}
	return nil
}

func (st *State) deleteAllowed(d *pb.DeleteRangeRequest) error {
	if err := st.auth.permitted(st.who, d.Key, d.RangeEnd, true); err != nil {
		return err
	}
	if d.PrevKv {
		return st.auth.permitted(st.who, d.Key, d.RangeEnd, false)
	}
	return nil
}

// txnAllowed is etcd's checkTxnPermission.
func (st *State) txnAllowed(t *pb.TxnRequest) error {
	for _, c := range t.Compare {
		if err := st.auth.permitted(st.who, c.Key, c.RangeEnd, false); err != nil {
			return err
		}
	}
	for _, ops := range [][]*pb.RequestOp{t.Success, t.Failure} {
		for _, o := range ops {
			var err error
			switch r := o.Request.(type) {
			case *pb.RequestOp_RequestRange:
				err = st.auth.permitted(st.who, r.RequestRange.GetKey(), r.RequestRange.GetRangeEnd(), false)
			case *pb.RequestOp_RequestPut:
				err = st.putAllowed(r.RequestPut.GetKey(), r.RequestPut.GetLease(), r.RequestPut.GetPrevKv())
			case *pb.RequestOp_RequestDeleteRange:
				if d := r.RequestDeleteRange; d != nil {
					if d.PrevKv {
						err = st.auth.permitted(st.who, d.Key, d.RangeEnd, false)
					}
					if err == nil {
						err = st.auth.permitted(st.who, d.Key, d.RangeEnd, true)
					}
				}
			case *pb.RequestOp_RequestTxn:
				if r.RequestTxn != nil {
					err = st.txnAllowed(r.RequestTxn)
				}
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// applyAuthEntry applies an auth request from the log and answers it.
func (n *Node) applyAuthEntry(b []byte, index uint64, p *pending) error {
	var r pb.InternalRaftRequest
	if err := proto.Unmarshal(b, &r); err != nil {
		return fmt.Errorf("%w: %w", errBadBatch, err)
	}
	var resp proto.Message
	var err error
	if r.DowngradeInfoSet != nil || r.ClusterVersionSet != nil {
		resp, err = n.applyVersionSet(&r)
	} else {
		resp, err = n.applyAuth(&r, index)
	}
	if err != nil && !isAnswer(err) {
		return err
	}
	if p != nil {
		rep := Reply{ID: p.req.ID, Resp: resp, Err: err, Rev: int64(n.state.auth.revision)}
		n.answer(p, rep, nil)
	}
	return nil
}

// AuthEnabled reports whether auth is on.
func (n *Node) AuthEnabled() bool { return n.state.auth.enabled }

// Authorize checks a call the driver serves itself (Status, Snapshot) as
// etcd's gRPC layer does: op says which.
func (n *Node) Authorize(c Caller, op proto.Message) error {
	ai, ok, err := n.identify(c)
	if errors.Is(err, errTokenNotApplied) {
		return rpctypes.ErrGRPCInvalidAuthToken
	}
	if err != nil {
		return err
	}
	if _, snapshot := op.(*pb.SnapshotRequest); snapshot {
		if !ok {
			return n.state.auth.admin(nil)
		}
		return n.state.auth.admin(&ai)
	}
	return n.authorize(op, ai, ok)
}

// watchAllowed is etcd's isWatchPermitted.
func (n *Node) watchAllowed(c Caller, key, end []byte) error {
	if !n.state.auth.enabled {
		return nil
	}
	ai, _, err := n.identify(c)
	if errors.Is(err, errTokenNotApplied) {
		err = rpctypes.ErrGRPCInvalidAuthToken
	}
	if err != nil {
		return err
	}
	return n.state.auth.permitted(ai, key, end, false)
}
