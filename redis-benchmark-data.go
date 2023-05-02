package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis" // 引入redis包
	"github.com/google/uuid"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	host        string
	port        int64
	keyPrefix   string
	commandsStr string
	commands    []string
	parallel    int64
	num         int64
	valLen      int64
	keyFrom     int64
)

var (
	batchName string
	keyValue  string
)

var (
	exeMap = map[string]func(no int64, conn redis.Conn) error{
		"SET":  execSet,
		"HSET": execHSet,
	}
)

func main() {
	var (
		now = time.Now()
	)

	flag.StringVar(&host, "h", "127.0.0.1", "redis host")
	flag.Int64Var(&port, "p", 6379, "redis port")
	flag.StringVar(&keyPrefix, "prefix", fmt.Sprintf("%s_%s_", now.Format("20060102150405"), uuid.New().String()[0:5]), "redis key prefix")
	flag.StringVar(&commandsStr, "c", "SET", "redis command")
	flag.Int64Var(&parallel, "parallel", 1, "parallel nums")
	flag.Int64Var(&num, "n", 10000, "startClient command nums")
	flag.Int64Var(&valLen, "len", 32, "value length")
	flag.Int64Var(&keyFrom, "key-from", 1, "key name from")
	flag.Parse()

	commands = strings.Split(commandsStr, ",")
	checkPrams()

	batchName = now.Format("2006-01-02-15:04:05") + "==" + keyPrefix

	for i := int64(0); i < valLen; i++ {
		keyValue += "x"
	}

	log.Printf("【%s】begin to exec! param = %s", batchName, paramStr())

	var wg sync.WaitGroup
	for i := int64(0); i < parallel; i++ {
		wg.Add(1)
		beg := time.Now().Second()
		go startClient(i, func(no int64, err error) {
			end := time.Now().Second()
			log.Printf("【%s】【goroutine %d】 startClient done, costs %d seconds, err = %v", batchName, no, end-beg, err)
			wg.Done()
		})
	}
	wg.Wait()

	end := time.Now().Second()
	log.Printf("【%s】end to exec! param = %s, costs %d", batchName, paramStr(), end-now.Second())
}

func checkPrams() {
	assert(host != "", "host should not be empty")
	assert(port > 0, "port should gte 0")

	expectCommands := map[string]struct{}{"HSET": {}, "SET": {}, "LPUSH": {}, "SADD": {}, "ZADD": {}}
	for _, command := range commands {
		_, ok := expectCommands[strings.ToUpper(command)]
		assert(ok, "commands should be one of HSET, SET, LPUSH, SADD or ZADD")
	}

	assert(parallel > 0 && parallel <= 100, "parallel should be 1~`100`")
	assert(num > 0, "num should be gt 0")
	assert(valLen > 0, "len should be gt 0")
	assert(keyFrom > 0, "keyFrom should be gt 0")
}

//host        string
//port        int64
//keyPrefix   string
//commandsStr string
//commands    []string
//parallel    int64
//num         int64
//valLen      int64
//keyFrom     int64
func paramStr() string {
	var sb strings.Builder
	sb.WriteString("   ==============   ")
	sb.WriteString("host=" + host + " || ")
	sb.WriteString("post=" + strconv.FormatInt(port, 10) + " || ")
	sb.WriteString("keyPrefix=" + keyPrefix + " || ")
	sb.WriteString("commandsStr=" + commandsStr + " || ")
	sb.WriteString("parallel=" + strconv.FormatInt(parallel, 10) + " || ")
	sb.WriteString("num=" + strconv.FormatInt(num, 10) + " || ")
	sb.WriteString("valLen=" + strconv.FormatInt(valLen, 10) + " || ")
	sb.WriteString("keyFrom=" + strconv.FormatInt(keyFrom, 10))
	sb.WriteString("   ==============   ")
	return sb.String()
}

func assert(b bool, message string) {
	if !b {
		panic(message)
	}
}

func startClient(no int64, def func(no int64, err error)) (err error) {
	defer def(no, err)

	var (
		conn redis.Conn
		now  = time.Now()
	)

	conn, err = redis.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatalf("【%s】redis.Dial err = %v, param=%s", batchName, err, paramStr())
		return
	}
	defer func() {
		conn.Close() // 关闭
		end := time.Now().Second()
		log.Printf("【%s】%d client all done, costs %d seconds, err=%v", batchName, no, end-now.Second(), err)
	}()

	for _, command := range commands {
		if execFun, ok := exeMap[strings.ToUpper(command)]; ok {
			err = execFun(no, conn)
			if err != nil {
				return
			}
		}
	}

	return
}

//assert(ok, "commands should be one of HSET, SET, LPUSH, SADD or ZADD")

func execSet(no int64, conn redis.Conn) error {
	beg := time.Now().Second()
	for i := keyFrom; i < num+keyFrom; i++ {
		key := fmt.Sprintf("%s_%d_%d_set", keyPrefix, no, i)
		_, err := conn.Do("Set", key, keyValue)
		if err != nil {
			log.Printf("【%s】【goroutine %d】 exec Set failed, num = %d, err = %v", batchName, no, num, err)
			return err
		}
		if i%1000 == 0 {
			end := time.Now().Second()
			log.Printf("【%s】【goroutine %d】 executing Set, executed nums = %d, key = %s, costs = %d", batchName, no, i, key, end-beg)
		}
	}
	log.Printf("【%s】【goroutine %d】 exec Set done, num = %d", batchName, no, num)
	return nil
}

func execHSet(no int64, conn redis.Conn) error {
	beg := time.Now().Second()
	for i := keyFrom; i < num+keyFrom; i++ {
		key := fmt.Sprintf("%s_%d_%d_hset", keyPrefix, no, i)
		_, err := conn.Do("HSet", key, i, keyValue)
		if err != nil {
			log.Printf("【%s】【goroutine %d】 exec HSet failed, num = %d, err = %v", batchName, no, num, err)
			return err
		}
		if i%1000 == 0 {
			end := time.Now().Second()
			log.Printf("【%s】【goroutine %d】 executing HSet, executed nums = %d, key = %s, subKey = %d, costs = %d", batchName, no, i, key, i, end-beg)
		}
	}
	end := time.Now().Second()
	log.Printf("【%s】【goroutine %d】 exec HSet done, num = %d, costs = %d", batchName, no, num, end-beg)
	return nil
}

//func execLPush(no int64) error {
//	for i := int64(0); i < num; i++ {
//		_, err := conn.Do("LPush", fmt.Sprintf("%s_%d", keyPrefix, i), keyValue, 1, 2, 3)
//		if err != nil {
//			log.Printf("【%s】【goroutine %d】 startClient LPush failed, num = %d, err = %v", batchName, no, num, err)
//			return err
//		}
//	}
//	log.Printf("【%s】【goroutine %d】 startClient LPush done, num = %d", batchName, no, num)
//	return nil
//}
