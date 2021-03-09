package helper

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
)

const (
	MetaService_CONF_PATH = "/etc/yigfsmeta/metaservice.toml"
)

type Config struct {
	TidbConfig            TidbConfig          `toml:"tidb_config"`
	MetaServiceConfig     MetaServiceConfig   `toml:"meta_service_config"`
}

type TidbConfig struct {
	MetaStore        string        `toml:"meta_store"`
	TidbInfo         string        `toml:"tidb_info"`
}

type MetaServiceConfig struct {
	Port              string        `toml:"port"`
	LogDir            string        `toml:"log_dir"`
	LogLevel          string        `toml:"log_level"`
	TlsKeyFile        string        `toml:"tls_key_file"`
	TlsCertFile       string        `toml:"tls_cert_file"`
}

var CONFIG Config

func SetupConfig() {
	MarshalTOMLConfig()
}

func MarshalTOMLConfig() error {
	data, err := ioutil.ReadFile(MetaService_CONF_PATH)
	if err != nil {
		if err != nil {
			panic("Cannot open metaservice.toml")
		}
	}
	var c Config
	_, err = toml.Decode(string(data), &c)
	if err != nil {
		panic("load metaservice.toml error: " + err.Error())
	}
	// setup CONFIG with defaults
	CONFIG.TidbConfig = c.TidbConfig
	CONFIG.MetaServiceConfig = c.MetaServiceConfig

	return nil
}



