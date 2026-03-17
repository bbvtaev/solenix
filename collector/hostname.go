package collector

import "os"

func getHostname() (string, error) {
	return os.Hostname()
}
