package tbredis

import (
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/extra/redisprometheus/v9"
	"github.com/redis/go-redis/v9"
	"github.com/testbook/tbredis/hooks"
	"github.com/testbook/tbredis/tracing"
)

type Config struct {
	*redis.ClusterOptions
	*redis.Options
	*redis.FailoverOptions
	KeyPrefix         string // prefix to all keys; example is "dev environment name"
	KeyDelimiter      string // delimiter to be used while appending keys; example is ":"
	KeyVarPlaceholder string // placeholder to be parsed using given arguments to obtain a final key; example is "?"
	Service           string // service name
	SubService        string // sub service name
}

type clusterClient struct {
	*redis.ClusterClient
}

type client struct {
	*redis.Client
}

type failoverClient struct {
	*redis.Client
}

var config Config

var c *clusterClient
var sc *client
var fc *failoverClient

func Init(conf Config) {
	config = conf
	c = new(clusterClient)
	c.ClusterClient = redis.NewClusterClient(conf.ClusterOptions)
	collector := redisprometheus.NewCollector(conf.Service, conf.SubService, c)
	prometheus.MustRegister(collector)
	c.ClusterClient.AddHook(hooks.NewHook())
	_ = tracing.InstrumentTracing(c.ClusterClient, tracing.WithDBStatement(true))
}

func InitClient(conf Config) {
	config = conf
	sc = new(client)
	sc.Client = redis.NewClient(conf.Options)
}

func InitFailoverClientWithSentinel(conf Config) {
	config = conf
	fc = new(failoverClient)
	fc.Client = redis.NewFailoverClient(conf.FailoverOptions)
}

func GetClient() *clusterClient {
	return c
}

func GetClientSingle() *client {
	return sc
}

func GetFailoverClient() *failoverClient {
	return fc
}

func ParseKey(key string, vars []string) (string, error) {
	arr := strings.Split(key, config.KeyVarPlaceholder)
	actualKey := ""
	if len(arr) != len(vars)+1 {
		return "", errors.New("tbredis: Insufficient arguments to parse key")
	} else {
		for index, val := range arr {
			if index == 0 {
				actualKey = arr[index]
			} else {
				actualKey += vars[index-1] + val
			}
		}
	}
	return getPrefixedKey(actualKey), nil
}

func getPrefixedKey(key string) string {
	return config.KeyPrefix + config.KeyDelimiter + key
}

func StripEnvKey(key string) string {
	return strings.TrimLeft(key, config.KeyPrefix+config.KeyDelimiter)
}

func SplitKey(key string) []string {
	return strings.Split(key, config.KeyDelimiter)
}
