package dbSync

import (
	"bufio"
	"github.com/davecgh/go-spew/spew"
	"io"
	"k8s.io/klog/v2"
	"net"
	"time"

	"github.com/alibaba/RedisShake/pkg/libs/atomic2"
	"github.com/alibaba/RedisShake/pkg/libs/io/pipe"
	"github.com/alibaba/RedisShake/redis-shake/base"
	utils "github.com/alibaba/RedisShake/redis-shake/common"

	conf "github.com/alibaba/RedisShake/redis-shake/configure"
)

// send command to source redis

func (ds *DbSyncer) sendSyncCmd(master, authType, passwd string, tlsEnable bool, tlsSkipVerify bool) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, authType, passwd, tlsEnable, tlsSkipVerify)
	for {
		select {
		case nsize := <-wait:
			if nsize == 0 {
				klog.Infof("DbSyncer[%d] + waiting source rdb", ds.id)
			} else {
				return c, nsize
			}
		case <-time.After(time.Second):
			klog.Infof("DbSyncer[%d] - waiting source rdb", ds.id)
		}
	}
}

func (ds *DbSyncer) sendPSyncCmd(master, authType, passwd string, tlsEnable bool, tlsSkipVerify bool, runId string,
	prevOffset int64) (pipe.Reader, int64, bool, string) {
	c := utils.OpenNetConn(master, authType, passwd, tlsEnable, tlsSkipVerify)
	klog.Infof("DbSyncer[%d] psync connect '%v' with auth type[%v] OK!", ds.id, master, authType)

	utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
	klog.Infof("DbSyncer[%d] psync send listening port[%v] OK!", ds.id, conf.Options.HttpProfile)

	// reader buffer bind to client
	br := bufio.NewReaderSize(c, utils.ReaderBufferSize)
	// writer buffer bind to client
	bw := bufio.NewWriterSize(c, utils.WriterBufferSize)

	klog.Infof("DbSyncer[%d] try to send 'psync' command: run-id[%v], offset[%v]", ds.id, runId, prevOffset)
	// send psync command and decode the result
	runid, offset, wait := utils.SendPSyncContinue(br, bw, runId, prevOffset, false)
	ds.stat.targetOffset.Set(offset)
	ds.fullSyncOffset = offset // store the full sync offset

	piper, pipew := pipe.NewSize(utils.ReaderBufferSize)
	if wait == nil {
		// continue
		klog.Infof("DbSyncer[%d] psync runid = %s, offset = %d, psync continue", ds.id, runId, offset)
		go ds.runIncrementalSync(c, br, bw, 0, runid, offset, master, authType, passwd, tlsEnable, pipew, true)
		return piper, 0, false, runid
	} else {
		// fullresync
		klog.Infof("DbSyncer[%d] psync runid = %s, offset = %d, fullsync", ds.id, runid, offset)

		// get rdb file size, wait source rdb dump successfully.
		var nsize int64
		for nsize == 0 {
			select {
			case nsize = <-wait:
				if nsize == 0 {
					klog.Infof("DbSyncer[%d] +", ds.id)
				}
			case <-time.After(time.Second):
				klog.Infof("DbSyncer[%d] -", ds.id)
			}
		}
		klog.Infof("DbSyncer[%d] psync runid = %s, offset = %d, fullsync", ds.id, runid, offset)
		go ds.runIncrementalSync(c, br, bw, int(nsize), runid, offset, master, authType, passwd, tlsEnable, pipew, true)
		return piper, nsize, true, runid
	}
}

func (ds *DbSyncer) runIncrementalSync(c net.Conn, br *bufio.Reader, bw *bufio.Writer, rdbSize int, runId string,
	offset int64, master, authType, passwd string, tlsEnable bool, pipew pipe.Writer,
	isFullSync bool) {
	// write -> pipew -> piper -> read
	defer pipew.Close()
	if isFullSync {
		p := make([]byte, 8192)
		// read rdb in for loop
		for rdbSize != 0 {
			// br -> pipew
			rdbSize -= utils.Iocopy(br, pipew, p, rdbSize)
		}
	}

	for {
		/*
		 * read from br(source redis) and write into pipew.
		 * Generally speaking, this function is forever run.
		 */
		klog.Infof("DbSyncer[%d] runIncrementalSync: runId[%v], offset[%v]", ds.id, runId, offset)
		n, err := ds.pSyncPipeCopy(c, br, bw, offset, pipew)
		if err != nil {
			klog.Exit(err, "DbSyncer[%d] psync runid = %s, offset = %d, pipe is broken",
				ds.id, runId, offset)
		}
		// the 'c' is closed every loop

		offset += n
		ds.stat.targetOffset.Set(offset)

		// reopen 'c' every time
		for {
			// ds.SyncStat.SetStatus("reopen")
			base.Status = "reopen"
			time.Sleep(time.Second)
			klog.Info("DbSyncer[%d] reopen connection", ds.id)
			// TODO: readtimeout and writetimeout should be configurable
			c = utils.OpenNetConnSoft(master, authType, passwd, tlsEnable)
			if c != nil {
				// klog.PurePrintf("%s\n", NewLogItem("SourceConnReopenSuccess", "INFO", LogDetail{Info: strconv.FormatInt(offset, 10)}))
				klog.Infof("DbSyncer[%d] Event:SourceConnReopenSuccess\tId: %s\toffset = %d",
					ds.id, conf.Options.Id, offset)
				// ds.SyncStat.SetStatus("incr")
				base.Status = "incr"
				break
			} else {
				// klog.PurePrintf("%s\n", NewLogItem("SourceConnReopenFail", "WARN", NewErrorLogDetail("", "")))
				klog.Errorf("DbSyncer[%d] Event:SourceConnReopenFail\tId: %s", ds.id, conf.Options.Id)
			}
		}
		klog.Infof("DbSyncer[%d] psync runid = %s, offset = %d, psync continue", ds.id, runId, offset)
		utils.AuthPassword(c, authType, passwd)
		klog.Infof("Send PSync Listening Port: %d", conf.Options.HttpProfile)
		utils.SendPSyncListeningPort(c, conf.Options.HttpProfile)
		br = bufio.NewReaderSize(c, utils.ReaderBufferSize)
		bw = bufio.NewWriterSize(c, utils.WriterBufferSize)
		klog.Infof("Send PSync Continue: %d, %d", runId, offset)
		utils.SendPSyncContinue(br, bw, runId, offset, true)
	}
}

func (ds *DbSyncer) pSyncPipeCopy(c net.Conn, br *bufio.Reader, bw *bufio.Writer, offset int64, copyto io.Writer) (int64, error) {
	var nread atomic2.Int64
	go func() {
		defer c.Close()
		for range time.NewTicker(1 * time.Second).C {
			select {
			case <-ds.WaitFull:
				klog.Infof("DbSyncer[%d] send PSync Ack: %d", ds.id, offset)
				if err := utils.SendPSyncAck(bw, offset+nread.Get()); err != nil {
					klog.Errorf("dbSyncer[%v] send offset to source redis failed[%v]", ds.id, err)
					return
				}
			default:
				klog.Infof("DbSyncer[%d] send PSync Ack: %d", ds.id, offset)
				if err := utils.SendPSyncAck(bw, 0); err != nil {
					klog.Errorf("dbSyncer[%v] send offset to source redis failed[%v]", ds.id, err)
					return
				}
			}
		}
	}()

	var p = make([]byte, 8192)
	for {
		n, err := br.Read(p)
		klog.V(5).Infof("DbSyncer[%d] read from source redis", ds.id)
		if err != nil {
			klog.Errorf("DbSyncer[%d] read from source redis failed[%v]", ds.id, err)
			return nread.Get(), nil
		}
		klog.V(5).Infof("DbSyncer[%d] write to target redis, bytes %v", ds.id, spew.Sdump(p[:n]))
		if _, err := copyto.Write(p[:n]); err != nil {
			klog.Errorf("DbSyncer[%d] write to target redis failed[%v]", ds.id, err)
			return nread.Get(), err
		}
		nread.Add(int64(n))
	}
}
