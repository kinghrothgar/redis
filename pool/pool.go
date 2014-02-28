package pool

import (
	"github.com/mediocregopher/radix/redis"
	"github.com/grooveshark/golib/gslog"
    "errors"
)

type Pool struct {
    // TODO: figure out channel direction
	outPool chan *Client
	inPool chan *Client
	versionChan chan string
    // TODO: Should network and addr be protect by a lock
	network, addr string
}

type Client struct {
	*redis.Client
	versionStr string
}

func buildVersionStr(network string, addr string) string {
	return network + addr
}

// flushPool removes and close all the clients on a pool channel
func flushPool(c chan *Client) {
	for {
		select {
		case client := <-c:
			client.Close()
			continue
		default:
			return
		}
	}
}

// poolMan is run in a go routine by new.  It moves clients from the inPool
// back to the outPool as long as the version string matches the current.  It
// also clears out the outPool when a new version string is recieved on the
// version channel.
func poolMan(pool *Pool, versionStr string) {
	for {
		// TODO: Do I need to break? I think I do
		select {
		case str := <-pool.versionChan:
			if str != versionStr {
				gslog.Debug("POOL: flushing pool because versionStr changed")
				versionStr = str
				flushPool(pool.outPool)
			}
			continue
		case client := <-pool.inPool:
			if client.versionStr == versionStr {
				gslog.Debug("POOL: poolMan added client back to pool")
				select {
                case pool.outPool <- client:
                    continue
                default:
                    gslog.Warn("POOL: out pool is full")
                }
                continue
			}
			client.Close()
			gslog.Debug("POOL: poolMan discarded client")
			continue
		}
	}
}

// New returns a redis client pool of size up to capacity with network and
// address configured.
func New(network string, addr string, capacity int) *Pool {
	var outPool, inPool chan *Client
	if capacity < 1 {
		outPool = make(chan *Client)
		inPool = make(chan *Client)
	} else {
		outPool = make(chan *Client, capacity)
		inPool = make(chan *Client, capacity)
	}

	pool := &Pool{
		outPool: outPool,
		inPool: inPool,
		versionChan: make(chan string),
		network: network,
		addr: addr,
	}
	go poolMan(pool, buildVersionStr(network, addr))
	return pool
}


// Get a *Client from the pool or create a new one if there are none available.
func (p *Pool) Get() (*Client, error) {
	select {
    // TODO: Should I be getting the ok also an verifying that the channel wasn't closed
	case r := <-p.outPool:
		gslog.Debug("POOL: got client from outPool")
		return r, nil
	default:
	}

	versionStr := buildVersionStr(p.network, p.addr)
	r, err := redis.Dial(p.network, p.addr)
	if err != nil {
	    return nil, err
    }
	// TODO: learn how to use a mix of named and unamed fields
	gslog.Debug("POOL: created new client")
	return &Client{r, versionStr}, nil
}

// Put a *Client back into the pool.  Clients that return errors should not be
// returned to the pool.
// Please don't give back clients that return errors :)
// Returns false on failure
func (p *Pool) Put(client *Client) error {
	select {
	case p.inPool <- client:
		return nil
	default:
        gslog.Warn("POOL: in pool is full")
        return errors.New("in pool is full")
	}
}

// Configure the network and address the pool is connecting too.  Changing
// either will cause the pool to be flushed out.
func (p *Pool) SetConnection(network string, addr string) {
	p.network = network
	p.addr = addr
	p.versionChan <- buildVersionStr(network, addr)
}
