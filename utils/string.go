package utils

import (
	"regexp"
	"strings"
)

var (
	oidExp = regexp.MustCompile(`([0-9a-fA-F]{24})`)
)

func GetKeyTemplate(s string) (so string) {
	s = strings.ReplaceAll(s, ":", " ")
	args := strings.SplitN(s, " ", 4)
	if len(args) < 3 {
		return
	}
	so = args[2]
	oids := oidExp.FindStringSubmatch(so)
	for _, o := range oids {
		so = strings.Replace(so, o, "{id}", -1)
	}
	return
}
