// Copyright 2015 CNI authors
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

package etcd

import (
	//	"fmt"
	"context"
	"encoding/json"
	"log"
	"net"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	//	"os"
	"backend"
	"backend/allocator"
	"path/filepath"
	"runtime"
	"strings"
)

const lastIPFilePrefix = "last_reserved_ip."

var (
	//dialTimeout       = 5 * time.Second
	requestTimeout    = 2 * time.Second
	//endpoints         = []string{"127.0.0.1:2379"}
	defaultSessionTTL = 60
)

// 将文件锁修改为etcd分布式锁
// key是ip地址，value是容器id
type Store struct {
	EtcdMutex *concurrency.Mutex
	Key       string
}



// Store implements the Store interface
var _ backend.Store = &Store{}

func New(n *allocator.IPAMConfig) (*Store, error) {
	etcd, err := ConnectStore(n.Endpoints)
	if err != nil {
		panic(err)
	}

	network, err := NetConfigJson(n)
	key, err := InitStore(n.Name, network, etcd)

	lk, err := Lock(n)
	if err != nil {
		return nil, err
	}

	return &Store{lk, key}, nil
}

func ConnectStore(endpoints []string) (etcd *clientv3.Client, err error) {
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	etcd, err = clientv3.New(config)
	if err != nil {
		panic(err)
	}
	defer etcd.Close()
	return etcd, err
}

//创建json格式的conf
func NetConfigJson(n *allocator.IPAMConfig) (config []byte, err error) {
	ip_set := allocator.IP_Settings{
		Gw:     n.Gateway,
		Net:    n.Subnet,
		Start:  n.RangeStart,
		End:    n.RangeEnd,
		Routes: n.Routes,
	}
	conf, err := json.Marshal(ip_set)
	return conf, err
}

// name network etcd(conn)  返回一个string 是一个key，key是name，value是network
func InitStore(k string, network []byte, etcd *clientv3) (store string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	netstring := string(network[:])
	resp, err := etcd.Put(ctx, k, netstring)
	defer cancel()
	if err != nil {
		panic(err)
	}
	return k, nil
}

func Lock(n *allocator.IPAMConfig) (*concurrency.Mutex, error) {
	config := clientv3.Config{
		Endpoints:   n.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	}

	client, err := clientv3.New(config)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	opts := &concurrency.sessionOptions{ttl: defaultSessionTTL, ctx: client.Ctx()}

	s, err := concurrency.NewSession(client, opts)
	if err != nil {
		panic(err)
	}

	m := concurrency.NewMutex(s, n.EtcdPrefix)

	return m, nil
}

func (s *Store) Reserve(id string, ip net.IP) (bool, error) {
	path := s.Key() + "/" + ip.String()
	last := s.Key() + "/" + "LastReservedIP"
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	etcd, err = clientv3.New(config)
	if err != nil {
		panic(err)
	}
	defer etcd.Close()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	_, err := etcd.Put(ctx, last, ip.string())
	//需要考虑last已存在时覆盖写的问题，相应处理机制还没看懂
	defer cancel()
	if err != nil {
		panic(err)
	}
	resp, err := etcd.Put(ctx, path, id)
	defer cancel()
	if err != nil {
		return false, err
	}

	return true, nil
}

// LastReservedIP returns the last reserved IP if exists
func (s *Store) LastReservedIP() (net.IP, error) {
	last := s.Key() + "/" + "LastReservedIP"
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	etcd, err = clientv3.New(config)
	if err != nil {
		panic(err)
	}
	defer etcd.Close()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	cancel()
	lastip, err := etcd.get(ctx, last)
	if err != nil {
		return nil, err
	}
	for _, ev := range lastip.Kvs {
		ip := ParseIP(ev.Key)
	}
	//    Kvs不知道是什么 ev 能获取到key和value ev.key是string
	return ip, nil
}

func (s *Store) Release(ip net.IP) error {
	path := s.Key() + "/" + ip.String()
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}

	etcd, err = clientv3.New(config)
	if err != nil {
		panic(err)
	}
	defer etcd.Close()
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := etcd.Delete(ctx, path)
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

// N.B. This function eats errors to be tolerant and
// release as much as possible
func (s *Store) ReleaseByID(id string) error {
	//实现从value查找key 然后delete key
	//文件的实现是从mynet根目录遍历读取ip，获取容器id，如果匹配，就删掉
	//etcd要实现应该也是类似，但没找到反向查询key的函数  etcd官方文档缺少详细的使用场景。
	return nil
}


func GetEscapedPath(dataDir string, fname string) string {
	if runtime.GOOS == "windows" {
		fname = strings.Replace(fname, ":", "_", -1)
	}
	return filepath.Join(dataDir, fname)
}

func (s *Store) Close() error {
}
