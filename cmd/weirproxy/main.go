package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/tidb-incubator/weir/pkg/config"
	"github.com/tidb-incubator/weir/pkg/proxy"
	"go.uber.org/zap"
)

var (
	configFilePath = flag.String("config", "conf/weirproxy.yaml", "weir proxy config file path")
)

func main() {
	flag.Parse()
	proxyConfigData, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		fmt.Printf("read config file error: %v\n", err)
		os.Exit(1)
	}

	proxyCfg, err := config.UnmarshalProxyConfig(proxyConfigData)
	if err != nil {
		fmt.Printf("parse config file error: %v\n", err)
		os.Exit(1)
	}

	p := proxy.NewProxy(proxyCfg)

	if err = p.Init(); err != nil {
		fmt.Printf("proxy init error: %v\n", err)
		p.Close()
		os.Exit(1)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
		syscall.SIGUSR1,
	)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				logutil.BgLogger().Warn("get os signal, close proxy server", zap.String("signal", sig.String()))
				p.Close()
				break
			} else {
				logutil.BgLogger().Warn("ignore os signal", zap.String("signal", sig.String()))
			}
		}
	}()

	if err := p.Run(); err != nil {
		logutil.BgLogger().Error("proxy run error, exit", zap.Error(err))
	}

	wg.Wait()
}
