package wl

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"maps"
	"slices"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Auth follows etcd v3.7.2's auth store (server/auth) and its checks
// (server/etcdserver/apply/auth.go, v3_server.go and api/v3rpc). Users,
// roles, whether auth is on and the auth revision are replicated: every
// auth request goes through Raft in a batch, as an InternalRaftRequest
// with the identity it came with, and checkpoints hold the store. A
// client is a user by a token from Authenticate, or by its certificate's
// common name. While auth is on, a write goes into its batch behind an op
// that names its user and auth revision, and is checked as it applies,
// on every node alike; reads and the other calls are checked when the
// node serves them. With auth off, none of this runs.

const (
	rootUser = "root"
	rootRole = "root"
)

// errNoPasswordUser is etcd's answer to a password for a user without
// one, which etcd's gRPC layer has no code for.
var errNoPasswordUser = &requestError{code: codeUnknown, msg: "auth: authentication failed, password was given for no password user"}

// authStore is the replicated part of etcd's auth store, with each user's
// merged permissions, rebuilt on every change.
type authStore struct {
	enabled  bool
	revision uint64
	users    map[string]*authpb.User
	roles    map[string]*authpb.Role
	perms    map[string]*userPerms
}

func newAuthStore() *authStore {
	return &authStore{revision: 1, users: map[string]*authpb.User{}, roles: map[string]*authpb.Role{}, perms: map[string]*userPerms{}}
}

// authInfo is who a request comes from: a user and the auth revision its
// identity was taken at. The zero authInfo is etcd's empty AuthInfo.
type authInfo struct {
	user     string
	revision uint64
}

// keySpan is a range of keys [begin, end); a nil end is open.
type keySpan struct{ begin, end []byte }

// userPerms are a user's read and write permissions, merged into sorted
// spans that do not touch.
type userPerms struct{ read, write []keySpan }

// rebuild recomputes every user's merged permissions, as etcd's
// refreshRangePermCache does after each change.
func (a *authStore) rebuild() {
	a.perms = map[string]*userPerms{}
	for name, u := range a.users {
		var read, write []keySpan
		for _, rn := range u.Roles {
			r := a.roles[rn]
			if r == nil {
				continue
			}
			for _, p := range r.KeyPermission {
				s := keySpan{begin: p.Key}
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
		a.perms[name] = &userPerms{read: mergeSpans(read), write: mergeSpans(write)}
	}
}

// mergeSpans sorts spans and joins those that overlap or touch.
func mergeSpans(ss []keySpan) []keySpan {
	slices.SortFunc(ss, func(x, y keySpan) int { return bytes.Compare(x.begin, y.begin) })
	var out []keySpan
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

// spansCover reports whether ss hold key, or the whole range [key, end)
// with Range's meaning of end, as etcd's interval tree does.
func spansCover(ss []keySpan, key, end []byte) bool {
	i, _ := slices.BinarySearchFunc(ss, key, func(s keySpan, k []byte) int {
		if bytes.Compare(s.begin, k) <= 0 {
			return -1
		}
		return 1
	})
	if i--; i < 0 {
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

func hasRoot(u *authpb.User) bool { return slices.Contains(u.Roles, rootRole) }

// The errors of etcd's auth, as its gRPC layer answers them.
var (
	errUserEmpty           = authErr(rpctypes.ErrGRPCUserEmpty)
	errAuthOldRevision     = authErr(rpctypes.ErrGRPCAuthOldRevision)
	errPermissionDenied    = authErr(rpctypes.ErrGRPCPermissionDenied)
	errUserNotFound        = authErr(rpctypes.ErrGRPCUserNotFound)
	errRoleNotFound        = authErr(rpctypes.ErrGRPCRoleNotFound)
	errAuthNotEnabled      = authErr(rpctypes.ErrGRPCAuthNotEnabled)
	errAuthFailed          = authErr(rpctypes.ErrGRPCAuthFailed)
	errInvalidAuthToken    = authErr(rpctypes.ErrGRPCInvalidAuthToken)
	errInvalidAuthMgmt     = authErr(rpctypes.ErrGRPCInvalidAuthMgmt)
	errRootUserNotExist    = authErr(rpctypes.ErrGRPCRootUserNotExist)
	errRootRoleNotExist    = authErr(rpctypes.ErrGRPCRootRoleNotExist)
	errUserAlreadyExist    = authErr(rpctypes.ErrGRPCUserAlreadyExist)
	errRoleAlreadyExist    = authErr(rpctypes.ErrGRPCRoleAlreadyExist)
	errRoleEmpty           = authErr(rpctypes.ErrGRPCRoleEmpty)
	errRoleNotGranted      = authErr(rpctypes.ErrGRPCRoleNotGranted)
	errPermissionNotGiven  = authErr(rpctypes.ErrGRPCPermissionNotGiven)
	errPermissionNotGrated = authErr(rpctypes.ErrGRPCPermissionNotGranted)
)

// authErr is one of etcd's gRPC errors as the node answers it.
func authErr(err error) *requestError {
	st := status.Convert(err)
	return &requestError{code: int(st.Code()), msg: st.Message()}
}

// permitted is etcd's isOpPermitted.
func (a *authStore) permitted(ai authInfo, key, end []byte, write bool) error {
	if !a.enabled {
		return nil
	}
	if ai.revision == 0 {
		return errUserEmpty
	}
	if ai.revision < a.revision {
		return errAuthOldRevision
	}
	u := a.users[ai.user]
	if u == nil {
		return errPermissionDenied
	}
	if hasRoot(u) {
		return nil
	}
	p := a.perms[ai.user]
	spans := p.read
	if write {
		spans = p.write
	}
	if spansCover(spans, key, end) {
		return nil
	}
	return errPermissionDenied
}

// admin is etcd's IsAdminPermitted; ai is nil without an identity.
func (a *authStore) admin(ai *authInfo) error {
	if !a.enabled {
		return nil
	}
	if ai == nil || ai.user == "" {
		return errUserEmpty
	}
	u := a.users[ai.user]
	if u == nil {
		return errUserNotFound
	}
	if !hasRoot(u) {
		return errPermissionDenied
	}
	return nil
}

func (a *authStore) hasRole(user, role string) bool {
	u := a.users[user]
	return u != nil && slices.Contains(u.Roles, role)
}

// changed counts an auth change: a new revision and new merged
// permissions.
func (a *authStore) changed() {
	a.revision++
	a.rebuild()
}

// validPermRange is etcd's isValidPermissionRange.
func validPermRange(key, end []byte) bool {
	switch {
	case len(key) == 0:
		return false
	case len(end) == 0, bytes.Compare(key, end) < 0:
		return true
	}
	return len(end) == 1 && end[0] == 0
}

// needsAdmin is etcd's needAdminPermission.
func needsAdmin(r *etcdpb.InternalRaftRequest) bool {
	return r.AuthEnable != nil || r.AuthDisable != nil || r.AuthUserAdd != nil || r.AuthUserDelete != nil ||
		r.AuthUserChangePassword != nil || r.AuthUserGrantRole != nil || r.AuthUserRevokeRole != nil ||
		r.AuthRoleAdd != nil || r.AuthRoleGrantPermission != nil || r.AuthRoleRevokePermission != nil ||
		r.AuthRoleDelete != nil || r.AuthUserList != nil || r.AuthRoleList != nil
}

// hashedPassword is the bcrypt hash a request carries, base64 encoded:
// the node that took the request hashed the password before proposing
// it, as etcd's does. nil means the hash does not decode.
func hashedPassword(hash string) []byte {
	b, err := base64.StdEncoding.DecodeString(hash)
	if err != nil || len(b) == 0 {
		return nil
	}
	return b
}

// applyAuth runs an auth request from the log as etcd's authApplierV3 and
// auth store do, with the identity its origin took it with. index is the
// entry's, which a simple token ends with.
func (n *node) applyAuth(r *etcdpb.InternalRaftRequest, index uint64) (proto.Message, error) {
	a := n.auth
	ai := authInfo{user: r.GetHeader().GetUsername(), revision: r.GetHeader().GetAuthRevision()}
	if needsAdmin(r) {
		if err := a.admin(&ai); err != nil {
			return nil, err
		}
	}
	h := n.respHeader()
	switch {
	case r.AuthEnable != nil:
		if a.enabled {
			return &etcdpb.AuthEnableResponse{Header: h}, nil
		}
		u := a.users[rootUser]
		if u == nil {
			return nil, errRootUserNotExist
		}
		if !hasRoot(u) {
			return nil, errRootRoleNotExist
		}
		a.enabled = true
		a.rebuild()
		return &etcdpb.AuthEnableResponse{Header: h}, nil
	case r.AuthDisable != nil:
		if a.enabled {
			a.enabled = false
			a.changed()
			clear(n.tokens)
		}
		return &etcdpb.AuthDisableResponse{Header: h}, nil
	case r.AuthStatus != nil:
		return &etcdpb.AuthStatusResponse{Header: h, Enabled: a.enabled, AuthRevision: a.revision}, nil
	case r.Authenticate != nil:
		return n.authenticated(r.Authenticate, index, h)
	case r.AuthUserAdd != nil:
		return a.userAdd(r.AuthUserAdd, h)
	case r.AuthUserDelete != nil:
		name := r.AuthUserDelete.Name
		if a.enabled && name == rootUser {
			return nil, errInvalidAuthMgmt
		}
		if a.users[name] == nil {
			return nil, errUserNotFound
		}
		delete(a.users, name)
		a.changed()
		n.dropTokens(name)
		return &etcdpb.AuthUserDeleteResponse{Header: h}, nil
	case r.AuthUserChangePassword != nil:
		c := r.AuthUserChangePassword
		u := a.users[c.Name]
		if u == nil {
			return nil, errUserNotFound
		}
		next := &authpb.User{Name: u.Name, Roles: u.Roles, Options: u.Options}
		if u.Options == nil || !u.Options.NoPassword {
			if next.Password = hashedPassword(c.HashedPassword); next.Password == nil {
				return nil, errNoPasswordUser
			}
		}
		a.users[c.Name] = next
		a.changed()
		n.dropTokens(c.Name)
		return &etcdpb.AuthUserChangePasswordResponse{Header: h}, nil
	case r.AuthUserGrantRole != nil:
		g := r.AuthUserGrantRole
		u := a.users[g.User]
		if u == nil {
			return nil, errUserNotFound
		}
		if g.Role != rootRole && a.roles[g.Role] == nil {
			return nil, errRoleNotFound
		}
		if i, found := slices.BinarySearch(u.Roles, g.Role); !found {
			u = proto.CloneOf(u)
			u.Roles = slices.Insert(u.Roles, i, g.Role)
			a.users[g.User] = u
			a.changed()
		}
		return &etcdpb.AuthUserGrantRoleResponse{Header: h}, nil
	case r.AuthUserRevokeRole != nil:
		rv := r.AuthUserRevokeRole
		if a.enabled && rv.Name == rootUser && rv.Role == rootRole {
			return nil, errInvalidAuthMgmt
		}
		u := a.users[rv.Name]
		if u == nil {
			return nil, errUserNotFound
		}
		i, found := slices.BinarySearch(u.Roles, rv.Role)
		if !found {
			return nil, errRoleNotGranted
		}
		u = proto.CloneOf(u)
		u.Roles = slices.Delete(u.Roles, i, i+1)
		a.users[rv.Name] = u
		a.changed()
		return &etcdpb.AuthUserRevokeRoleResponse{Header: h}, nil
	case r.AuthUserGet != nil:
		if err := a.admin(&ai); err != nil && r.AuthUserGet.Name != ai.user {
			return nil, err
		}
		u := a.users[r.AuthUserGet.Name]
		if u == nil {
			return nil, errUserNotFound
		}
		return &etcdpb.AuthUserGetResponse{Header: h, Roles: slices.Clone(u.Roles)}, nil
	case r.AuthUserList != nil:
		return &etcdpb.AuthUserListResponse{Header: h, Users: slices.Sorted(maps.Keys(a.users))}, nil
	case r.AuthRoleAdd != nil:
		name := r.AuthRoleAdd.Name
		if name == "" {
			return nil, errRoleEmpty
		}
		if a.roles[name] != nil {
			return nil, errRoleAlreadyExist
		}
		a.roles[name] = &authpb.Role{Name: []byte(name)}
		// etcd counts the change but rebuilds nothing: no user has the
		// role yet.
		a.revision++
		return &etcdpb.AuthRoleAddResponse{Header: h}, nil
	case r.AuthRoleGet != nil:
		role := r.AuthRoleGet.Role
		if err := a.admin(&ai); err != nil && !a.hasRole(ai.user, role) {
			return nil, err
		}
		ro := a.roles[role]
		if ro == nil {
			return nil, errRoleNotFound
		}
		resp := &etcdpb.AuthRoleGetResponse{Header: h}
		if role == rootRole {
			resp.Perm = []*authpb.Permission{{PermType: authpb.Permission_READWRITE, Key: []byte{}, RangeEnd: []byte{0}}}
		} else {
			for _, p := range ro.KeyPermission {
				resp.Perm = append(resp.Perm, proto.CloneOf(p))
			}
		}
		return resp, nil
	case r.AuthRoleList != nil:
		return &etcdpb.AuthRoleListResponse{Header: h, Roles: slices.Sorted(maps.Keys(a.roles))}, nil
	case r.AuthRoleDelete != nil:
		name := r.AuthRoleDelete.Role
		if a.enabled && name == rootRole {
			return nil, errInvalidAuthMgmt
		}
		if a.roles[name] == nil {
			return nil, errRoleNotFound
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
		return &etcdpb.AuthRoleDeleteResponse{Header: h}, nil
	case r.AuthRoleGrantPermission != nil:
		g := r.AuthRoleGrantPermission
		if g.Perm == nil {
			return nil, errPermissionNotGiven
		}
		if !validPermRange(g.Perm.Key, g.Perm.RangeEnd) {
			return nil, errInvalidAuthMgmt
		}
		ro := a.roles[g.Name]
		if ro == nil {
			return nil, errRoleNotFound
		}
		ro = proto.CloneOf(ro)
		i := slices.IndexFunc(ro.KeyPermission, func(p *authpb.Permission) bool { return bytes.Compare(p.Key, g.Perm.Key) >= 0 })
		if i >= 0 && bytes.Equal(ro.KeyPermission[i].Key, g.Perm.Key) && bytes.Equal(ro.KeyPermission[i].RangeEnd, g.Perm.RangeEnd) {
			ro.KeyPermission[i].PermType = g.Perm.PermType
		} else {
			ro.KeyPermission = append(ro.KeyPermission, &authpb.Permission{Key: g.Perm.Key, RangeEnd: g.Perm.RangeEnd, PermType: g.Perm.PermType})
			slices.SortStableFunc(ro.KeyPermission, func(x, y *authpb.Permission) int { return bytes.Compare(x.Key, y.Key) })
		}
		a.roles[g.Name] = ro
		a.changed()
		return &etcdpb.AuthRoleGrantPermissionResponse{Header: h}, nil
	case r.AuthRoleRevokePermission != nil:
		rv := r.AuthRoleRevokePermission
		ro := a.roles[rv.Role]
		if ro == nil {
			return nil, errRoleNotFound
		}
		keep := &authpb.Role{Name: ro.Name}
		for _, p := range ro.KeyPermission {
			if !bytes.Equal(p.Key, rv.Key) || !bytes.Equal(p.RangeEnd, rv.RangeEnd) {
				keep.KeyPermission = append(keep.KeyPermission, p)
			}
		}
		if len(keep.KeyPermission) == len(ro.KeyPermission) {
			return nil, errPermissionNotGrated
		}
		a.roles[rv.Role] = keep
		a.changed()
		return &etcdpb.AuthRoleRevokePermissionResponse{Header: h}, nil
	}
	return nil, badRequest(errors.New("an auth request with nothing in it"))
}

// userAdd is etcd's UserAdd. The node that took it hashed the password.
func (a *authStore) userAdd(r *etcdpb.AuthUserAddRequest, h *etcdpb.ResponseHeader) (proto.Message, error) {
	if r.Name == "" {
		return nil, errUserEmpty
	}
	if a.users[r.Name] != nil {
		return nil, errUserAlreadyExist
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
	return &etcdpb.AuthUserAddResponse{Header: h}, nil
}

// respHeader is a response's header as a message.
func (n *node) respHeader() *etcdpb.ResponseHeader {
	return &etcdpb.ResponseHeader{ClusterId: n.clusterID, MemberId: n.id, Revision: n.st.rev, RaftTerm: n.term}
}

// Simple tokens follow etcd's tokenSimple: the node that takes an
// Authenticate picks a random prefix, and every node, when the entry
// applies, makes "<prefix>.<entry index>" a token of the user. A token
// not used for --auth-token-ttl goes. Tokens are not replicated state: a
// restart forgets them, as etcd's does.

// simpleToken is a token's user and when it expires.
type simpleToken struct {
	user    string
	expires time.Time
}

// authenticated applies an InternalAuthenticateRequest: the node that
// took it checked the password.
func (n *node) authenticated(r *etcdpb.InternalAuthenticateRequest, index uint64, h *etcdpb.ResponseHeader) (proto.Message, error) {
	a := n.auth
	if !a.enabled {
		return nil, errAuthNotEnabled
	}
	u := a.users[r.Name]
	if u == nil || (u.Options != nil && u.Options.NoPassword) {
		return nil, errAuthFailed
	}
	resp := &etcdpb.AuthenticateResponse{Header: h}
	if r.SimpleToken != "" {
		resp.Token = r.SimpleToken + "." + strconv.FormatUint(index, 10)
		n.tokens[resp.Token] = &simpleToken{user: r.Name, expires: n.now.Add(n.tokenTTL)}
	}
	return resp, nil
}

// dropTokens forgets the simple tokens of user.
func (n *node) dropTokens(user string) {
	for t, st := range n.tokens {
		if st.user == user {
			delete(n.tokens, t)
		}
	}
}

// expireTokens forgets the simple tokens not used in time.
func (n *node) expireTokens() {
	for t, st := range n.tokens {
		if !n.now.Before(st.expires) {
			delete(n.tokens, t)
		}
	}
}

// encode appends the store as a checkpoint holds it: whether auth is
// on, its revision, then the users and the roles, each a length and its
// message.
func (a *authStore) encode(b []byte) []byte {
	on := uint64(0)
	if a.enabled {
		on = 1
	}
	b = binary.AppendUvarint(b, on)
	b = binary.AppendUvarint(b, a.revision)
	b = binary.AppendUvarint(b, uint64(len(a.users)))
	for _, name := range slices.Sorted(maps.Keys(a.users)) {
		m, _ := proto.MarshalOptions{Deterministic: true}.Marshal(a.users[name])
		b = binary.AppendUvarint(b, uint64(len(m)))
		b = append(b, m...)
	}
	b = binary.AppendUvarint(b, uint64(len(a.roles)))
	for _, name := range slices.Sorted(maps.Keys(a.roles)) {
		m, _ := proto.MarshalOptions{Deterministic: true}.Marshal(a.roles[name])
		b = binary.AppendUvarint(b, uint64(len(m)))
		b = append(b, m...)
	}
	return b
}

// readAuth reads what encode wrote.
func (cr *ckptReader) readAuth() (*authStore, error) {
	a := newAuthStore()
	on, err := cr.uvarint()
	if err != nil {
		return nil, err
	}
	a.enabled = on != 0
	if a.revision, err = cr.uvarint(); err != nil {
		return nil, err
	}
	for _, into := range []func([]byte) error{
		func(b []byte) error {
			u := &authpb.User{}
			if err := proto.Unmarshal(b, u); err != nil {
				return err
			}
			a.users[string(u.Name)] = u
			return nil
		},
		func(b []byte) error {
			r := &authpb.Role{}
			if err := proto.Unmarshal(b, r); err != nil {
				return err
			}
			a.roles[string(r.Name)] = r
			return nil
		},
	} {
		k, err := cr.uvarint()
		if err != nil {
			return nil, err
		}
		for range k {
			l, err := cr.uvarint()
			if err != nil {
				return nil, err
			}
			b, err := cr.bytes(l)
			if err != nil {
				return nil, err
			}
			if err := into(b); err != nil {
				return nil, err
			}
		}
	}
	a.rebuild()
	return a, nil
}
