package wl

import (
	"crypto/rand"
	"encoding/base64"
	"math/big"
	"time"

	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
)

// The Auth service: each call goes through Raft as an InternalRaftRequest
// (auth.go). bcrypt hashes and checks passwords, and JWTs are signed, off
// the loop: they take milliseconds.

// authMethod is an Auth method's request, and where it goes in an
// InternalRaftRequest.
type authMethod struct {
	path string
	req  func() proto.Message
	set  func(r *etcdpb.InternalRaftRequest, m proto.Message)
}

func authCall[T any, P interface {
	*T
	proto.Message
}](path string, set func(*etcdpb.InternalRaftRequest, P)) authMethod {
	return authMethod{
		path: "/etcdserverpb.Auth/" + path,
		req:  func() proto.Message { return P(new(T)) },
		set:  func(r *etcdpb.InternalRaftRequest, m proto.Message) { set(r, m.(P)) },
	}
}

// authMethods are the Auth service's methods, by kind.
var authMethods = map[uint8]authMethod{
	kAuthEnable:  authCall("AuthEnable", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthEnableRequest) { r.AuthEnable = q }),
	kAuthDisable: authCall("AuthDisable", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthDisableRequest) { r.AuthDisable = q }),
	kAuthStatus:  authCall("AuthStatus", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthStatusRequest) { r.AuthStatus = q }),
	kAuthenticate: authCall("Authenticate", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthenticateRequest) {
		r.Authenticate = &etcdpb.InternalAuthenticateRequest{Name: q.Name}
	}),
	kUserAdd:    authCall("UserAdd", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserAddRequest) { r.AuthUserAdd = q }),
	kUserGet:    authCall("UserGet", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserGetRequest) { r.AuthUserGet = q }),
	kUserList:   authCall("UserList", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserListRequest) { r.AuthUserList = q }),
	kUserDelete: authCall("UserDelete", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserDeleteRequest) { r.AuthUserDelete = q }),
	kUserChangePassword: authCall("UserChangePassword", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserChangePasswordRequest) {
		r.AuthUserChangePassword = q
	}),
	kUserGrantRole:  authCall("UserGrantRole", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserGrantRoleRequest) { r.AuthUserGrantRole = q }),
	kUserRevokeRole: authCall("UserRevokeRole", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthUserRevokeRoleRequest) { r.AuthUserRevokeRole = q }),
	kRoleAdd:        authCall("RoleAdd", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleAddRequest) { r.AuthRoleAdd = q }),
	kRoleGet:        authCall("RoleGet", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleGetRequest) { r.AuthRoleGet = q }),
	kRoleList:       authCall("RoleList", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleListRequest) { r.AuthRoleList = q }),
	kRoleDelete:     authCall("RoleDelete", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleDeleteRequest) { r.AuthRoleDelete = q }),
	kRoleGrantPermission: authCall("RoleGrantPermission", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleGrantPermissionRequest) {
		r.AuthRoleGrantPermission = q
	}),
	kRoleRevokePermission: authCall("RoleRevokePermission", func(r *etcdpb.InternalRaftRequest, q *etcdpb.AuthRoleRevokePermissionRequest) {
		r.AuthRoleRevokePermission = q
	}),
}

func authKind(kind uint8) bool { return kind >= kAuthEnable && kind <= kRoleRevokePermission }

// authLogin is an Authenticate under way on its origin: the password, and
// the auth revision it was checked at.
type authLogin struct {
	name, password string
	revision       uint64
}

// identify is etcd's AuthInfoFromCtx: the user of a request from its
// token, or its certificate's common name. ok is false without one.
func (n *node) identify(c *client, s *stream) (authInfo, bool, error) {
	a := n.auth
	if a.enabled && s.token != "" {
		if n.jwt != nil {
			user, rev, err := n.jwt.Info(s.token)
			if err != nil {
				return authInfo{}, false, errInvalidAuthToken
			}
			return authInfo{user: user, revision: rev}, true, nil
		}
		st := n.tokens[s.token]
		if st == nil {
			return authInfo{}, false, errInvalidAuthToken
		}
		st.expires = n.now.Add(n.tokenTTL)
		return authInfo{user: st.user, revision: a.revision}, true, nil
	}
	if cn := c.nameOf(s); cn != "" {
		return authInfo{user: cn, revision: a.revision}, true, nil
	}
	return authInfo{}, false, nil
}

// nameOf is the certificate name of the client of s: of the client a
// side call is for, on the side calls' own client.
func (c *client) nameOf(s *stream) string {
	if s.byCaller {
		return s.caller
	}
	return c.cn
}

// identityOf is what the certificate name of the client of s adds to the
// size of its writes.
func (c *client) identityOf(s *stream) int {
	if s.byCaller {
		return identityBytes(s.caller)
	}
	return c.identity
}

// authRequest takes an Auth call.
func (n *node) authRequest(c *client, s *stream, msg []byte) error {
	m := authMethods[s.kind]
	req := m.req()
	if err := proto.Unmarshal(msg, req); err != nil {
		n.reject(c, s, badRequest(err))
		return nil
	}
	ai, ok, err := n.identify(c, s)
	if err != nil {
		n.reject(c, s, err)
		return nil
	}
	r := &etcdpb.InternalRaftRequest{}
	if ok {
		r.Header = &etcdpb.RequestHeader{Username: ai.user, AuthRevision: ai.revision}
	}
	m.set(r, req)
	h := mkHandle(c, s.id)
	switch q := req.(type) {
	case *etcdpb.AuthenticateRequest:
		n.login(h, r, authLogin{name: q.Name, password: q.Password})
		return nil
	case *etcdpb.AuthUserAddRequest:
		if q.Options == nil || !q.Options.NoPassword {
			n.hashThenPropose(h, r, q.Password, func(hash string) { q.HashedPassword, q.Password = hash, "" })
			return nil
		}
	case *etcdpb.AuthUserChangePasswordRequest:
		if q.Password != "" {
			n.hashThenPropose(h, r, q.Password, func(hash string) { q.HashedPassword, q.Password = hash, "" })
			return nil
		}
	}
	return n.proposeAuth(h, r)
}

// proposeAuth puts an auth request into the turn's batch.
func (n *node) proposeAuth(h handle, r *etcdpb.InternalRaftRequest) error {
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	n.addOp(h, kAuthOp, b)
	return nil
}

// hashThenPropose hashes password off the loop, puts the hash in r with
// set, and proposes r.
func (n *node) hashThenPropose(h handle, r *etcdpb.InternalRaftRequest, password string, set func(hash string)) {
	cost := n.bcryptCost
	n.offLoop(func() func() {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
		return func() {
			if err != nil {
				n.replyErrorH(h, codeInternal, err.Error())
				return
			}
			set(base64.StdEncoding.EncodeToString(hash))
			if err := n.proposeAuth(h, r); err != nil {
				n.replyErrorH(h, codeInternal, err.Error())
			}
		}
	})
}

// login is etcd's Authenticate: it checks the password against the
// user's hash off the loop, then has the log take the Authenticate,
// which makes a simple token with a prefix it picks. If auth changed in
// between, it starts again.
func (n *node) login(h handle, r *etcdpb.InternalRaftRequest, l authLogin) {
	a := n.auth
	if !a.enabled {
		n.replyErrorH(h, errAuthNotEnabled.code, errAuthNotEnabled.msg)
		return
	}
	u := a.users[l.name]
	if u == nil {
		n.replyErrorH(h, errAuthFailed.code, errAuthFailed.msg)
		return
	}
	if u.Options != nil && u.Options.NoPassword {
		n.replyErrorH(h, errNoPasswordUser.code, errNoPasswordUser.msg)
		return
	}
	l.revision = a.revision
	hash := u.Password
	n.offLoop(func() func() {
		err := bcrypt.CompareHashAndPassword(hash, []byte(l.password))
		var prefix string
		if err == nil && n.jwt == nil {
			prefix, err = tokenPrefix()
		}
		return func() {
			if err != nil {
				n.replyErrorH(h, errAuthFailed.code, errAuthFailed.msg)
				return
			}
			r.Authenticate.SimpleToken = prefix
			n.logins[h] = l
			if err := n.proposeAuth(h, r); err != nil {
				delete(n.logins, h)
				n.replyErrorH(h, codeInternal, err.Error())
			}
		}
	})
}

// tokenPrefix is a simple token's random prefix: 16 letters, as etcd's
// genTokenPrefix picks.
func tokenPrefix() (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 16)
	for i := range b {
		k, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		b[i] = letters[k.Int64()]
	}
	return string(b), nil
}

// applyAuthOp applies an auth request from a batch at index, and answers
// it on its origin.
func (n *node) applyAuthOp(req []byte, index uint64, c *client, s *stream) error {
	var r etcdpb.InternalRaftRequest
	if err := proto.Unmarshal(req, &r); err != nil {
		return badRequest(err)
	}
	resp, err := n.applyAuth(&r, index)
	if c == nil {
		return err
	}
	h := mkHandle(c, s.id)
	l, login := n.logins[h]
	delete(n.logins, h)
	if err != nil {
		return err
	}
	if login && n.auth.revision != l.revision {
		// Auth changed after the password was checked: check it again.
		n.login(h, &etcdpb.InternalRaftRequest{Header: r.Header, Authenticate: &etcdpb.InternalAuthenticateRequest{Name: l.name}}, l)
		return nil
	}
	if login && n.jwt != nil {
		// etcd signs when the entry applies, with the auth revision then.
		ar, _ := resp.(*etcdpb.AuthenticateResponse)
		jwt, name, rev := n.jwt, l.name, n.auth.revision
		n.offLoop(func() func() {
			token, err := jwt.Sign(name, rev)
			return func() {
				if err != nil {
					n.replyErrorH(h, errAuthFailed.code, errAuthFailed.msg)
					return
				}
				ar.Token = token
				n.replyMsgH(h, ar)
			}
		})
		return nil
	}
	n.replyMsgH(h, resp)
	return nil
}

// replyMsgH answers the call of h with the message m.
func (n *node) replyMsgH(h handle, m proto.Message) {
	c, s := n.clientOf(h)
	if c == nil {
		return
	}
	b, err := proto.Marshal(m)
	if err != nil {
		n.replyError(c, s.id, codeInternal, err.Error())
		return
	}
	var w msgW
	n.beginReply(c, s, &w, len(b))
	w.write(b)
	n.endReply(c, s, &w)
}

// offLoop runs work off the loop and what it returns on the loop; with
// no loop around the node, as in tests, at once.
func (n *node) offLoop(work func() func()) {
	if n.shell == nil {
		work()()
		return
	}
	n.shell.offLoop(func() func() error {
		done := work()
		return func() error {
			done()
			return nil
		}
	})
}

// defaultTokenTTL is etcd's --auth-token-ttl default.
const defaultTokenTTL = 300 * time.Second
