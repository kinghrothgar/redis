package pool

import "github.com/fzzy/radix/redis"

type Pool struct {
	pool chan *redis.Client
	network string
	addr string
}

func New(network, addr string, capacity int) *Pool {
	var pool chan *redis.Client
	if capacity < 1 {
		pool = make(chan *redis.Client)
	} else {
		pool = make(chan *redis.Client, capacity)
	}

	return &Pool{
		pool: pool,
		network: network,
		addr: addr,
	}
}

// Get a *redis.Client from the pool
// Returns nil on failure
func (p *Pool) Get() *redis.Client {
	select {
	case r := <-p.pool:
		return r
	default:
	}

	if r, err := redis.Dial(p.network, p.addr); err == nil {
		return r
	}

	return nil
}

// Put a *redis.Client into the pool
// Please don't give back clients that return errors :)
// Returns false on failure
func (p *Pool) Put(r *redis.Client) bool {
	select {
	case p.pool <- r:
		return true
	default:
		return false
	}
}
