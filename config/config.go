package config

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

type Config struct {
	DefaultIndex string `toml:"default-index"`
	URL          string `toml:"url"`
}

func confPath() string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return filepath.Join(u.HomeDir, ".esbuddy.toml")
}

func LoadConfig() Config {
	tml, err := ioutil.ReadFile(confPath())
	if err != nil && os.IsNotExist(err) {
		return Config{}
	} else if err != nil {
		panic(err)
	}

	var conf Config
	err = toml.Unmarshal(tml, &conf)
	if err != nil {
		panic(err)
	}

	return conf
}
