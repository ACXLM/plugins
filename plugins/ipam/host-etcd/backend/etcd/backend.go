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
	"net"
	"errors"
	"time"
	"context"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/containernetworking/plugins/plugins/ipam/host-etcd/backend"
)

const lastIPFilePrefix = "last_reserved_ip."

// var defaultDataDir = "/var/lib/cni/networks"
var defaultDataDir = "/ipam"

// Store is a simple etcd-backed store that creates one kv pair per IP
// address. The value of the pair is the container ID.
type Store struct {
	mutex *concurrency.Mutex
	kv clientv3.KV
}

// Store implements the Store interface
var _ backend.Store = &Store{}

func New(network string, endPoints []string) (*Store, error) {
	if len(endPoints) == 0 {
		return nil, errors.New("No available endpoints for etcd client")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	session, err := concurrency.NewSession(cli)
	if err != nil {
		return nil, err
	}

	mutex := concurrency.NewMutex(session, "/ipam/lock")
	kv := clientv3.NewKV(cli)
	
	return &Store{mutex, kv}, nil
}

func (s *Store) Lock() error {
	return s.mutex.Lock(context.TODO())
}

func (s *Store) Unlock() error {
	return s.mutex.Unlock(context.TODO())
}

func (s *Store) Close() error {
	// TODO:
	return nil
}

func (s *Store) Reserve(id string, ip net.IP, rangeID string) (bool, error) {

	if _, err := s.kv.Put(context.TODO(), "/ipam/ips/" + ip.String(),
		strings.TrimSpace(id)); err != nil {
		// TODO: txn
		return false, nil
	}
	
	// store the reserved ip in lastIPFile
	if _, err := s.kv.Put(context.TODO(), "/ipam/last_reserved_ip" + rangeID,
		ip.String()); err != nil {
		return false, err
	}
	return true, nil
}

// LastReservedIP returns the last reserved IP if exists
func (s *Store) LastReservedIP(rangeID string) (net.IP, error) {
	resp, err := s.kv.Get(context.TODO(), "/ipam/last_reserved_ip" + rangeID)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.New("Exactly one IP expected to get from last reserved")
	}
	return net.ParseIP(string(resp.Kvs[0].Value)), nil
}

func (s *Store) Release(ip net.IP) error {
	_, err := s.kv.Delete(context.TODO(), "/ipam/ips/" + ip.String())
	return err
}

// N.B. This function eats errors to be tolerant and
// release as much as possible
func (s *Store) ReleaseByID(id string) error {
	resp, err := s.kv.Get(context.TODO(), "/ipam/ips/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, item := range resp.Kvs {
		if strings.TrimSpace(string(item.Value)) == strings.TrimSpace(id) {
			_, err = s.kv.Delete(context.TODO(), "/ipam/ips/" + strings.TrimSpace(string(item.Key)))
			return err
		}
	}
	return nil
}
