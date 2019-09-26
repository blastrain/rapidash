package rapidash

import (
	"fmt"
	"io/ioutil"
	"time"

	"golang.org/x/xerrors"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Rule *RuleConfig `yaml:"rule"`
	SLC  *SLCConfig  `yaml:"slc"`
	LLC  *LLCConfig  `yaml:"llc"`
}

type RuleConfig struct {
	Servers           *[]string           `yaml:"servers"`
	Logger            *LoggerConfig       `yaml:"logger"`
	Retry             *RetryConfig        `yaml:"retry"`
	CacheControl      *CacheControlConfig `yaml:"cache_control"`
	Timeout           *int                `yaml:"timeout"`
	MaxIdleConnection *int                `yaml:"max_idle_connection"`
}

type LoggerConfig struct {
	Mode    *string `yaml:"mode"`
	Enabled *bool   `yaml:"enabled"`
}

type RetryConfig struct {
	Limit    *int           `yaml:"limit"`
	Interval *time.Duration `yaml:"interval"`
}

type CacheControlConfig struct {
	OptimisticLock  *bool `yaml:"optimistic_lock"`
	PessimisticLock *bool `yaml:"pessimistic_lock"`
}

type SLCConfig struct {
	Servers        *ServersConfig           `yaml:"servers"`
	Tables         *map[string]*TableConfig `yaml:"tables"`
	Expiration     *time.Duration           `yaml:"expiration"`
	LockExpiration *time.Duration           `yaml:"lock_expiration"`
}

type TableConfig struct {
	ShardKey       *string             `yaml:"shard_key"`
	Server         *ServerConfig       `yaml:"server"`
	CacheControl   *CacheControlConfig `yaml:"cache_control"`
	Expiration     *time.Duration      `yaml:"expiration"`
	LockExpiration *time.Duration      `yaml:"lock_expiration"`
}

type LLCConfig struct {
	Servers        *ServersConfig         `yaml:"servers"`
	Tags           *map[string]*TagConfig `yaml:"tags"`
	CacheControl   *CacheControlConfig    `yaml:"cache_control"`
	Expiration     *time.Duration         `yaml:"expiration"`
	LockExpiration *time.Duration         `yaml:"lock_expiration"`
}

type TagConfig struct {
	Server         *ServerConfig  `yaml:"server"`
	Expiration     *time.Duration `yaml:"expiration"`
	LockExpiration *time.Duration `yaml:"lock_expiration"`
}

type ServersConfig struct {
	Type  CacheServerType `yaml:"type"`
	Addrs []string        `yaml:"addrs"`
}

type ServerConfig struct {
	Type CacheServerType `yaml:"type"`
	Addr string          `yaml:"addr"`
}

func NewConfig(path string) (*Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, xerrors.Errorf("cannot read file %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal from %s: %w", string(file), err)
	}
	return &cfg, nil
}

func (cfg *Config) Options() []OptionFunc {
	opts := []OptionFunc{}
	opts = append(opts, cfg.Rule.Options()...)
	if cfg.SLC != nil {
		opts = append(opts, cfg.SLC.Options()...)
	}
	if cfg.LLC != nil {
		opts = append(opts, cfg.LLC.Options()...)
	}
	return opts
}

func (cfg *RuleConfig) Options() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.Servers != nil {
		opts = append(opts, ServerAddrs(*cfg.Servers))
	}
	if cfg.MaxIdleConnection != nil {
		opts = append(opts, MaxIdleConnections(*cfg.MaxIdleConnection))
	}
	opts = append(opts, cfg.Logger.Options()...)
	opts = append(opts, cfg.Retry.Options()...)
	opts = append(opts, cfg.CacheControl.SLCOptions()...)
	opts = append(opts, cfg.CacheControl.LLCOptions()...)
	return opts
}

func (cfg *LoggerConfig) Options() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.Mode != nil {
		switch *cfg.Mode {
		case "console":
			opts = append(opts, LogMode(LogModeConsole))
		case "json":
			opts = append(opts, LogMode(LogModeJSON))
		case "server":
			opts = append(opts, LogMode(LogModeServerDebug))
		}
	}
	if cfg.Enabled != nil {
		opts = append(opts, LogEnabled(*cfg.Enabled))
	}
	return opts
}

func (cfg *RetryConfig) Options() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.Limit != nil {
		opts = append(opts, MaxRetryCount(*cfg.Limit))
	}
	if cfg.Interval != nil {
		opts = append(opts, RetryInterval(*cfg.Interval))
	}
	return opts
}

func (cfg *CacheControlConfig) SLCOptions() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.OptimisticLock != nil {
		opts = append(opts, SecondLevelCacheOptimisticLock(*cfg.OptimisticLock))
	}
	if cfg.PessimisticLock != nil {
		opts = append(opts, SecondLevelCachePessimisticLock(*cfg.PessimisticLock))
	}
	return opts
}

func (cfg *CacheControlConfig) LLCOptions() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.OptimisticLock != nil {
		opts = append(opts, LastLevelCacheOptimisticLock(*cfg.OptimisticLock))
	}
	if cfg.PessimisticLock != nil {
		opts = append(opts, LastLevelCachePessimisticLock(*cfg.PessimisticLock))
	}
	return opts
}

func (cfg *CacheControlConfig) TableOptions(table string) []OptionFunc {
	opts := []OptionFunc{}
	if cfg.OptimisticLock != nil {
		opts = append(opts, SecondLevelCacheTableOptimisticLock(table, *cfg.OptimisticLock))
	}
	if cfg.PessimisticLock != nil {
		opts = append(opts, SecondLevelCacheTablePessimisticLock(table, *cfg.PessimisticLock))
	}
	return opts
}

func (cfg *SLCConfig) Options() []OptionFunc {
	opts := []OptionFunc{}
	fmt.Printf("================= cfg.Servers:%v\n", cfg.Servers)
	if cfg.Servers != nil {
		opts = append(opts, SecondLevelCacheServers(*cfg.Servers))
	}
	if cfg.Tables != nil {
		for table, tableCfg := range *cfg.Tables {
			opts = append(opts, tableCfg.Options(table)...)
		}
	}
	if cfg.Expiration != nil {
		opts = append(opts, SecondLevelCacheExpiration(*cfg.Expiration))
	}
	if cfg.LockExpiration != nil {
		opts = append(opts, SecondLevelCacheLockExpiration(*cfg.LockExpiration))
	}
	return opts
}

func (cfg *TableConfig) Options(table string) []OptionFunc {
	opts := []OptionFunc{}
	if cfg.ShardKey != nil {
		opts = append(opts, SecondLevelCacheTableShardKey(table, *cfg.ShardKey))
	}
	if cfg.Server != nil {
		opts = append(opts, SecondLevelCacheTableServer(table, *cfg.Server))
	}
	if cfg.Expiration != nil {
		opts = append(opts, SecondLevelCacheTableExpiration(table, *cfg.Expiration))
	}
	if cfg.LockExpiration != nil {
		opts = append(opts, SecondLevelCacheTableLockExpiration(table, *cfg.LockExpiration))
	}
	if cfg.CacheControl != nil {
		opts = append(opts, cfg.CacheControl.TableOptions(table)...)
	}
	return opts
}

func (cfg *LLCConfig) Options() []OptionFunc {
	opts := []OptionFunc{}
	if cfg.Servers != nil {
		opts = append(opts, LastLevelCacheServer(*cfg.Servers))
	}
	if cfg.Tags != nil {
		for tag, tagCfg := range *cfg.Tags {
			opts = append(opts, tagCfg.Options(tag)...)
		}
	}
	if cfg.Expiration != nil {
		opts = append(opts, LastLevelCacheExpiration(*cfg.Expiration))
	}
	if cfg.LockExpiration != nil {
		opts = append(opts, LastLevelCacheLockExpiration(*cfg.LockExpiration))
	}
	if cfg.CacheControl != nil {
		opts = append(opts, cfg.CacheControl.LLCOptions()...)
	}
	return opts
}

func (cfg *TagConfig) Options(tag string) []OptionFunc {
	var opts []OptionFunc
	if cfg.Server != nil {
		opts = append(opts, LastLevelCacheTagServer(tag, *cfg.Server))
	}
	if cfg.Expiration != nil {
		opts = append(opts, LastLevelCacheTagExpiration(tag, *cfg.Expiration))
	}
	if cfg.LockExpiration != nil {
		opts = append(opts, LastLevelCacheTagLockExpiration(tag, *cfg.LockExpiration))
	}
	return opts
}
