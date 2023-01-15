// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/pkg/v3/cobrautl"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/mirror"

	"github.com/spf13/cobra"
)

var (
	cdinsecureTr   bool
	cdcert         string
	cdkey          string
	cdcacert       string
	cdprefix       string
	cddestprefix   string
	cduser         string
	cdpassword     string
	cdnodestprefix bool
)

// NewMakeMirrorCheckDataCommand returns the cobra command for "makeMirror".
func NewMakeMirrorCheckDataCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "make-mirror-check-data [options] <destination>",
		Short: "check mirror data at the destination etcd cluster",
		Run:   makeMirrorCheckDataCommandFunc,
	}

	c.Flags().StringVar(&cdprefix, "prefix", "", "Key-value prefix to mirror")
	c.Flags().StringVar(&cddestprefix, "dest-prefix", "", "destination prefix to mirror a prefix to a different prefix in the destination cluster")
	c.Flags().BoolVar(&cdnodestprefix, "no-dest-prefix", false, "mirror key-values to the root of the destination cluster")
	c.Flags().StringVar(&cdcert, "dest-cert", "", "Identify secure client using this TLS certificate file for the destination cluster")
	c.Flags().StringVar(&cdkey, "dest-key", "", "Identify secure client using this TLS key file")
	c.Flags().StringVar(&cdcacert, "dest-cacert", "", "Verify certificates of TLS enabled secure servers using this CA bundle")
	// TODO: secure by default when etcd enables secure gRPC by default.
	c.Flags().BoolVar(&cdinsecureTr, "dest-insecure-transport", true, "Disable transport security for client connections")
	c.Flags().StringVar(&cduser, "dest-user", "", "Destination username[:password] for authentication (prompt if password is not supplied)")
	c.Flags().StringVar(&cdpassword, "dest-password", "", "Destination password for authentication (if this option is used, --user option shouldn't include password)")

	return c
}

func makeMirrorCheckDataCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, errors.New("make-mirror takes one destination argument"))
	}

	dialTimeout := dialTimeoutFromCmd(cmd)
	keepAliveTime := keepAliveTimeFromCmd(cmd)
	keepAliveTimeout := keepAliveTimeoutFromCmd(cmd)
	sec := &secureCfg{
		cert:              cdcert,
		key:               cdkey,
		cacert:            cdcacert,
		insecureTransport: cdinsecureTr,
	}

	auth := authDestCfg()

	cc := &clientConfig{
		endpoints:        []string{args[0]},
		dialTimeout:      dialTimeout,
		keepAliveTime:    keepAliveTime,
		keepAliveTimeout: keepAliveTimeout,
		scfg:             sec,
		acfg:             auth,
	}
	dc := cc.mustClient()
	c := mustClientFromCmd(cmd)

	err := makeMirrorCheckData(context.TODO(), c, dc)
	cobrautl.ExitWithError(cobrautl.ExitError, err)
}

func makeMirrorCheckData(ctx context.Context, c *clientv3.Client, dc *clientv3.Client) error {
	total := int64(0)
	diff := int64(0)
	notFound := int64(0)
	ctx2, cancel := context.WithCancel(context.TODO())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
	loop:
		for {
			select {
			case <-time.NewTicker(2 * time.Second).C:
				fmt.Println("total key number: ", atomic.LoadInt64(&total))
				fmt.Println("diff value key number: ", atomic.LoadInt64(&diff))
				fmt.Println("not found key number: ", atomic.LoadInt64(&notFound))
			case <-ctx.Done():
				break loop
			case <-ctx2.Done():
				break loop
			}
		}
		fmt.Println("total key number: ", atomic.LoadInt64(&total))
		fmt.Println("diff value key number: ", atomic.LoadInt64(&diff))
		fmt.Println("not found key number: ", atomic.LoadInt64(&notFound))
		wg.Done()
	}()

	resp, err := c.Get(ctx, "foo")
	if err != nil {
		fmt.Println("get from source cluster failed: ", err)
		return err
	}
	sourceRev := resp.Header.Revision
	resp, err = dc.Get(ctx, "foo")
	if err != nil {
		fmt.Println("get from destination cluster failed: ", err)
		return err
	}
	destRev := resp.Header.Revision

	s := mirror.NewSyncer(c, cdprefix, sourceRev)
	rc, errc := s.SyncBase(ctx)

	// if destination prefix is specified and remove destination prefix is true return error
	if cdnodestprefix && len(cddestprefix) > 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("`--dest-prefix` and `--no-dest-prefix` cannot be set at the same time, choose one"))
	}

	// if remove destination prefix is false and destination prefix is empty set the value of destination prefix same as prefix
	if !cdnodestprefix && len(cddestprefix) == 0 {
		cddestprefix = cdprefix
	}

	opts := []clientv3.OpOption{clientv3.WithRev(destRev)}
	for r := range rc {
		for _, kv := range r.Kvs {
			atomic.AddInt64(&total, 1)
			resp, err := dc.Get(ctx, modifyPrefix(string(kv.Key)), opts...)
			if err != nil {
				return err
			}
			found := false
			for _, destkv := range resp.Kvs {
				if string(destkv.Key) == modifyPrefix(string(kv.Key)) {
					found = true
					if string(destkv.Value) != string(kv.Value) {
						atomic.AddInt64(&diff, 1)
						fmt.Printf("different value, source key(%s), source value: %s, dest value: %s\n", kv.Key, string(kv.Value), string(destkv.Value))
					}
				}
			}
			if !found {
				atomic.AddInt64(&notFound, 1)
				fmt.Printf("not found key, source key(%s), source value: %s\n", kv.Key, string(kv.Value))
			}
		}
	}

	cancel()
	wg.Wait()
	err = <-errc
	if err != nil {
		return err
	}
	return nil
}
