package zfs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/influxdb/telegraf/internal"
	"github.com/influxdb/telegraf/plugins"
)

type Zfs struct {
	KstatPath    string
	KstatMetrics []string
	PoolMetrics  bool
}

type poolInfo struct {
	name       string
	ioFilename string
}

var sampleConfig = `
  # ZFS kstat path
  # If not specified, then default is:
  # kstatPath = "/proc/spl/kstat/zfs"
  #
  # By default, telegraf gather all zfs stats
  # If not specified, then default is:
  # kstatMetrics = ["arcstats", "zfetchstats", "vdev_cache_stats"]
  #
  # By default, don't gather zpool stats
  # poolMetrics = false
`

func (z *Zfs) SampleConfig() string {
	return sampleConfig
}

func (z *Zfs) Description() string {
	return "Read metrics of ZFS from arcstats, zfetchstats and vdev_cache_stats"
}

func getPools(kstatPath string) []poolInfo {
	pools := make([]poolInfo, 0)
	poolsDirs, _ := filepath.Glob(kstatPath + "/*/io")

	for _, poolDir := range poolsDirs {
		poolDirSplit := strings.Split(poolDir, "/")
		pool := poolDirSplit[len(poolDirSplit)-2]
		pools = append(pools, poolInfo{name: pool, ioFilename: poolDir})
	}

	return pools
}

func getTags(pools []poolInfo) map[string]string {
	var poolNames string

	for _, pool := range pools {
		if len(poolNames) != 0 {
			poolNames += "::"
		}
		poolNames += pool.name
	}

	return map[string]string{"pools": poolNames}
}

func gatherPoolStats(pool poolInfo, acc plugins.Accumulator) error {
	lines, err := internal.ReadLines(pool.ioFilename)
	if err != nil {
		return err
	}

	if len(lines) != 3 {
		return err
	}

	keys := strings.Fields(lines[1])
	values := strings.Fields(lines[2])

	keyCount := len(keys)

	if keyCount != len(values) {
		return fmt.Errorf("Key and value count don't match Keys:%v Values:%v", keys, values)
	}

	tag := map[string]string{"pool": pool.name}

	for i := 0; i < keyCount; i++ {
		value, err := strconv.ParseInt(values[i], 10, 64)
		if err != nil {
			return err
		}

		acc.Add(keys[i], value, tag)
	}

	return nil
}

func (z *Zfs) Gather(acc plugins.Accumulator) error {
	kstatMetrics := z.KstatMetrics
	if len(kstatMetrics) == 0 {
		kstatMetrics = []string{"arcstats", "zfetchstats", "vdev_cache_stats"}
	}

	kstatPath := z.KstatPath
	if len(kstatPath) == 0 {
		kstatPath = "/proc/spl/kstat/zfs"
	}

	pools := getPools(kstatPath)
	tags := getTags(pools)

	if z.PoolMetrics {
		for _, pool := range pools {
			err := gatherPoolStats(pool, acc)
			if err != nil {
				return err
			}
		}
	}

	for _, metric := range kstatMetrics {
		lines, err := internal.ReadLines(kstatPath + "/" + metric)
		if err != nil {
			return err
		}
		for i, line := range lines {
			if i == 0 || i == 1 {
				continue
			}
			if len(line) < 1 {
				continue
			}
			rawData := strings.Split(line, " ")
			key := metric + "_" + rawData[0]
			rawValue := rawData[len(rawData)-1]
			value, _ := strconv.ParseInt(rawValue, 10, 64)
			acc.Add(key, value, tags)
		}
	}
	return nil
}

func init() {
	plugins.Add("zfs", func() plugins.Plugin {
		return &Zfs{}
	})
}
