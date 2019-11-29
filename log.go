package rapidash

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	zerolog "github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"go.knocknote.io/rapidash/server"
)

type Logger interface {
	Warn(msg string)
	Add(string, server.CacheKey, LogEncoder)
	Get(string, SLCType, server.CacheKey, LogEncoder)
	GetFromDB(string, string, interface{}, LogEncoder)
	GetMulti(string, SLCType, []server.CacheKey, LogEncoder)
	Set(string, SLCType, server.CacheKey, LogEncoder)
	InsertIntoDB(string, string, interface{}, LogEncoder)
	Update(string, SLCType, server.CacheKey, LogEncoder)
	UpdateForDB(string, string, interface{}, LogEncoder)
	Delete(string, SLCType, server.CacheKey)
	DeleteFromDB(string, string)
}

var (
	log         Logger = &DefaultLogger{}
	isNopLogger        = false
)

type SLCType string

const (
	SLCServer SLCType = "server"
	SLCStash  SLCType = "stash"
	SLCDB     SLCType = "db"
)

type SLCCommandType string

const (
	SLCCommandGet      SLCCommandType = "get"
	SLCCommandSet      SLCCommandType = "set"
	SLCCommandGetMulti SLCCommandType = "get_multi"
	SLCCommandUpdate   SLCCommandType = "update"
	SLCCommandDelete   SLCCommandType = "delete"
	SLCCommandAdd      SLCCommandType = "add"
)

const (
	red     = 31
	green   = 32
	yellow  = 33
	blue    = 34
	magenta = 35
	cyan    = 36
)

func setNopLogger() {
	log = &NopLogger{}
	zlog.Logger = zerolog.Nop()
	isNopLogger = true
}

func setConsoleLogger() {
	log = &DefaultLogger{isConsole: true}
	zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	isNopLogger = false
}

func setJSONLogger() {
	log = &DefaultLogger{}
	zlog.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	isNopLogger = false
}

func init() {
	zerolog.TimeFieldFormat = ""
}

type LogString string

func (s LogString) EncodeLog() string {
	return string(s)
}

type LogStrings []server.CacheKey

func (l LogStrings) EncodeLog() string {
	ss := make([]string, len(l))
	for idx, s := range l {
		ss[idx] = s.String()
	}
	return strings.Join(ss, ",")
}

type LogMap map[string]interface{}

func (m LogMap) EncodeLog() string {
	bytes, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(bytes)
}

type LogEncoder interface {
	EncodeLog() string
}

type DefaultLogger struct {
	isConsole bool
}

func (*DefaultLogger) Warn(msg string) {
	zlog.Warn().Msg(msg)
}

func (*DefaultLogger) msgWithColor(code int, msg string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", code, msg)
}

func (dl *DefaultLogger) msg(cmd SLCCommandType, text string) string {
	if !dl.isConsole {
		return text
	}
	switch cmd {
	case SLCCommandGet:
		return dl.msgWithColor(green, text)
	case SLCCommandGetMulti:
		return dl.msgWithColor(magenta, text)
	case SLCCommandSet:
		return dl.msgWithColor(cyan, text)
	case SLCCommandUpdate:
		return dl.msgWithColor(yellow, text)
	case SLCCommandDelete:
		return dl.msgWithColor(red, text)
	case SLCCommandAdd:
		return dl.msgWithColor(blue, text)
	}
	return text
}

func (dl *DefaultLogger) Add(id string, key server.CacheKey, value LogEncoder) {
	zlog.Info().
		Str("id", id).
		Str("command", "add").
		Str("type", string(SLCServer)).
		Str("key", key.String()).
		Str("value", value.EncodeLog()).
		Msg(dl.msg(SLCCommandAdd, "----add-------[stash]---->[server]"))
}

func (dl *DefaultLogger) Get(id string, typ SLCType, key server.CacheKey, value LogEncoder) {
	l := zlog.Info().
		Str("id", id).
		Str("command", "get").
		Str("type", string(typ)).
		Str("key", key.String()).
		Str("value", value.EncodeLog())
	switch typ {
	case SLCServer:
		l.Msg(dl.msg(SLCCommandGet, "<----get------[stash]-----[server]"))
	case SLCStash:
		l.Msg(dl.msg(SLCCommandGet, "<----get------[stash]     [server]"))
	}
}

func (dl *DefaultLogger) GetFromDB(id, sql string, args interface{}, value LogEncoder) {
	zlog.Info().
		Str("id", id).
		Str("command", "get").
		Str("type", string(SLCDB)).
		Str("key", sql).
		Interface("args", args).
		Str("value", value.EncodeLog()).
		Msg(dl.msg(SLCCommandGet, "<----get------[stash]-----[db]"))
}

func (dl *DefaultLogger) GetMulti(id string, typ SLCType, key []server.CacheKey, value LogEncoder) {
	keys := make([]string, len(key))
	for idx, k := range key {
		keys[idx] = k.String()
	}
	l := zlog.Info().
		Str("id", id).
		Str("command", "get_multi").
		Str("type", string(typ)).
		Strs("key", keys).
		Str("value", value.EncodeLog())
	switch typ {
	case SLCServer:
		l.Msg(dl.msg(SLCCommandGetMulti, "<--get_multi--[stash]-----[server]"))
	case SLCStash:
		l.Msg(dl.msg(SLCCommandGetMulti, "<--get_multi--[stash]     [server]"))
	case SLCDB:
		l.Msg(dl.msg(SLCCommandGetMulti, "<--get_multi--[stash]-----[db]"))
	}
}

func (dl *DefaultLogger) Set(id string, typ SLCType, key server.CacheKey, value LogEncoder) {
	l := zlog.Info().
		Str("id", id).
		Str("command", "set").
		Str("type", string(typ)).
		Str("key", key.String()).
		Str("value", value.EncodeLog())
	switch typ {
	case SLCServer:
		l.Msg(dl.msg(SLCCommandSet, "----set-------[stash]---->[server]"))
	case SLCStash:
		l.Msg(dl.msg(SLCCommandSet, "----set------>[stash]     [server]"))
	}
}

func (dl *DefaultLogger) InsertIntoDB(id, sql string, args interface{}, value LogEncoder) {
	zlog.Info().
		Str("id", id).
		Str("command", "set").
		Str("type", string(SLCDB)).
		Str("key", sql).
		Interface("args", args).
		Str("value", value.EncodeLog()).
		Msg(dl.msg(SLCCommandSet, "----set-------[stash]---->[db]"))
}

func (dl *DefaultLogger) Update(id string, typ SLCType, key server.CacheKey, value LogEncoder) {
	l := zlog.Info().
		Str("id", id).
		Str("command", "update").
		Str("type", string(typ)).
		Str("key", key.String()).
		Str("value", value.EncodeLog())
	switch typ {
	case SLCServer:
		l.Msg(dl.msg(SLCCommandUpdate, "---update-----[stash]---->[server]"))
	case SLCStash:
		l.Msg(dl.msg(SLCCommandUpdate, "---update---->[stash]     [server]"))
	}
}

func (dl *DefaultLogger) UpdateForDB(id, sql string, args interface{}, value LogEncoder) {
	zlog.Info().
		Str("id", id).
		Str("command", "update").
		Str("type", string(SLCDB)).
		Str("key", sql).
		Interface("args", args).
		Str("value", value.EncodeLog()).
		Msg(dl.msg(SLCCommandUpdate, "---update-----[stash]---->[db]"))
}

func (dl *DefaultLogger) Delete(id string, typ SLCType, key server.CacheKey) {
	l := zlog.Info().
		Str("id", id).
		Str("command", "delete").
		Str("type", string(typ)).
		Str("key", key.String())
	switch typ {
	case SLCServer:
		l.Msg(dl.msg(SLCCommandDelete, "---delete-----[stash]---->[server]"))
	case SLCStash:
		l.Msg(dl.msg(SLCCommandDelete, "---delete---->[stash]     [server]"))
	}
}

func (dl *DefaultLogger) DeleteFromDB(id, sql string) {
	zlog.Info().
		Str("id", id).
		Str("command", "delete").
		Str("type", string(SLCDB)).
		Str("key", sql).
		Msg(dl.msg(SLCCommandDelete, "---delete-----[stash]---->[db]"))
}

type NopLogger struct{}

func (*NopLogger) Warn(msg string)                                                          {}
func (*NopLogger) Add(id string, key server.CacheKey, value LogEncoder)                     {}
func (*NopLogger) Get(id string, typ SLCType, key server.CacheKey, value LogEncoder)        {}
func (*NopLogger) GetFromDB(id, sql string, args interface{}, value LogEncoder)             {}
func (*NopLogger) GetMulti(id string, typ SLCType, key []server.CacheKey, value LogEncoder) {}
func (*NopLogger) Set(id string, typ SLCType, key server.CacheKey, value LogEncoder)        {}
func (*NopLogger) InsertIntoDB(id, sql string, args interface{}, value LogEncoder)          {}
func (*NopLogger) Update(id string, typ SLCType, key server.CacheKey, value LogEncoder)     {}
func (*NopLogger) UpdateForDB(id, sql string, args interface{}, value LogEncoder)           {}
func (*NopLogger) Delete(id string, typ SLCType, key server.CacheKey)                       {}
func (*NopLogger) DeleteFromDB(id, sql string)                                              {}
