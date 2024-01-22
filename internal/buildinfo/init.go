package buildinfo

import (
	"runtime/debug"
	"time"
)

const (
	projectPackage = "github.com/ch-rollup/ch-rollup"
)

var (
	info *Info
)

func Get() *Info {
	return info
}

func getVersion(m *debug.Module) (string, bool) {
	if m == nil || m.Path != projectPackage {
		return "", false
	}
	return m.Version, true
}

func initInfo() {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	info = &Info{
		GoVersion: buildInfo.GoVersion,
	}

	var isDep bool
	if version, ok := getVersion(&buildInfo.Main); ok {
		info.Version = version
	} else {
		isDep = true
		for _, m := range buildInfo.Deps {
			if v, ok := getVersion(m); ok {
				info.Version = v
				break
			}
		}
	}

	if !isDep {
		for _, setting := range buildInfo.Settings {
			switch setting.Key {
			case "vcs.revision":
				info.Commit = setting.Value
			case "vcs.time":
				if t, err := time.Parse(time.RFC3339Nano, setting.Value); err == nil {
					info.Time = t
				}
			}
		}
	}
}

func init() {
	initInfo()
}
