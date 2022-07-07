package dbSync

import "time"

var (
	incrSyncReadeTimeout = time.Duration(20) * time.Second
	incrSyncWriteTimeout = time.Duration(20) * time.Second
)

type delayNode struct {
	t  time.Time // timestamp
	id int64     // id
}

type cmdDetail struct {
	Cmd    string
	Args   []interface{}
	Offset int64
	Db     int
}

func (c *cmdDetail) String() string {
	str := c.Cmd
	for _, s := range c.Args {
		str += " " + string(s.([]byte))
	}
	return str
}
