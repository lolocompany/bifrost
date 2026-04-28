package process

import (
	"net"
	"os"
)

func FreeTCPAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer func() {
		_ = ln.Close()
	}()
	return ln.Addr().String(), nil
}

func ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
