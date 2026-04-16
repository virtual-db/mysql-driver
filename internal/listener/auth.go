package listener

import (
	"net"

	vitessmysql "github.com/dolthub/vitess/go/mysql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// passthroughAuthServer is a Vitess AuthServer that admits all users without
// credential verification. Authentication was already performed by the auth proxy.
type passthroughAuthServer struct {
	methods []vitessmysql.AuthMethod
}

// NewPassthroughAuthServer returns an AuthServer that admits all users.
func NewPassthroughAuthServer() vitessmysql.AuthServer {
	s := &passthroughAuthServer{}
	s.methods = []vitessmysql.AuthMethod{&passthroughAuthMethod{}}
	return s
}

func (s *passthroughAuthServer) AuthMethods() []vitessmysql.AuthMethod {
	return s.methods
}

func (s *passthroughAuthServer) DefaultAuthMethodDescription() vitessmysql.AuthMethodDescription {
	return vitessmysql.MysqlNativePassword
}

type passthroughAuthMethod struct{}

func (m *passthroughAuthMethod) Name() vitessmysql.AuthMethodDescription {
	return vitessmysql.MysqlNativePassword
}

func (m *passthroughAuthMethod) HandleUser(_ *vitessmysql.Conn, _ string) bool {
	return true
}

func (m *passthroughAuthMethod) AllowClearTextWithoutTLS() bool {
	return true
}

func (m *passthroughAuthMethod) AuthPluginData() ([]byte, error) {
	return make([]byte, 20), nil
}

func (m *passthroughAuthMethod) HandleAuthPluginData(
	_ *vitessmysql.Conn,
	_ string,
	_ []byte,
	_ []byte,
	_ net.Addr,
) (vitessmysql.Getter, error) {
	return &passthroughGetter{}, nil
}

type passthroughGetter struct{}

func (g *passthroughGetter) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{}
}
