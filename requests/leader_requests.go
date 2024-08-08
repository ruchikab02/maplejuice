package rqs

import (
	"encoding/json"
	"fmt"
	"maple_juice/common"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// data structures to store leader-relevant data
var file_task_q = make(map[string][]*common.Request)
var overall_task_q []*common.Request

var in_progress_tq = make(map[string]map[string]int)
var file_locks = make(map[string]*common.File)
var sdfs_f = make(map[string][]string) // maps sdfs filename to vm that stores it
var file_writes = make(map[string]int)
var file_reads = make(map[string]int)
var fcounts = make(map[string]int) // maps vm name to file count
var line_counts = make(map[string]int)
var mem_list []string

// mutexes for shared resources
var ftq_lock sync.RWMutex
var otq_lock sync.RWMutex
var ipq_lock sync.RWMutex
var sf_lock sync.RWMutex
var fw_lock sync.RWMutex
var fr_lock sync.RWMutex
var fc_lock sync.RWMutex
var lc_lock sync.RWMutex
var ml_lock sync.RWMutex
var file_lock sync.RWMutex

// Opens a listener on the leader port to accept request objects
func OpenLeaderListener() {
	// Listens via TCP connection
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.LEADER_PORT)
	if err != nil {
		fmt.Println("Could not connect to server:", err)
		os.Exit(1)
	}
	defer int_listener.Close()
	// accept a request
	for {
		conn, err := int_listener.Accept()
		if err != nil {
			fmt.Println("Could not accept a request:", err)
			return
		}
		// getting a request object and adding it to the task queue
		var req *common.Request
		rec := make([]byte, common.BUF_SIZE)
		bytes_read, err := conn.Read(rec)
		err = json.Unmarshal(rec[:bytes_read], &req)
		if err != nil {
			fmt.Println("unmarshaling error listener", err)
		}
		if req.IsFinished == "false" {
			// fmt.Println("adding request")
			// common.PrintRequest(req)
			if validateRequest(req) {
				EditFileTaskQ("add", req)
				EditOTaskQ("add", req)
				// fmt.Println("added request")
			}
			// if strings.Contains(req.Sdfs_name, "juice_out/") {
			// 	fmt.Println("adding request to" , req.Command ,req.Sdfs_name)
			// }

		} else {
			if req.IsFinished == "true" {
				if req.Req_type != "N" {
					// fmt.Println("finished request")
					// common.PrintRequest(req)

					UnlockFile(req.Sdfs_name, req.Req_type)
					fmt.Println("unlocking file")
					// fmt.Println("after unlockFile")
					// remove it from task_q
					EditFileTaskQ("rm", req)
					EditOTaskQ("delete", req)
					if req.Command == "delete" {
						DeleteFileLocks(req.Sdfs_name)
					}
				}

			}
		}
		defer conn.Close()
	}
}

// function that runs periodically to check the queue
// to determine whether or not to execute a task
func CheckQueue() {
	for {
		otq_lock.Lock()
		var req *common.Request
		var isExecutable bool
		if len(overall_task_q) != 0 {
			for i := 0; i < len(overall_task_q); i++ {
				req = overall_task_q[i]
				// check if request can be executed
				isExecutable = CheckIfExecutable(req)
				// fmt.Println("isexecutable:", isExecutable)
				if isExecutable && req.Req_type != "N" {
					fmt.Println("Locking file")
					LockFile(req.Sdfs_name, req.Req_type)
				}
				if isExecutable {
					// fmt.Println("lalalalal")
					fmt.Println("task to execute", req.Command)
					HandleRequest(req)
					// pop off the task queue for ls
					if req.Command == "ls" {
						EditFileTaskQ("rm", req)
						overall_task_q = common.RemoveAtIndex(overall_task_q, i)
					}
					break
				}
			}

		}
		otq_lock.Unlock()
		time.Sleep(time.Second * time.Duration(common.TGOSSIP))
	}
}

func CheckIfExecutable(req *common.Request) bool {
	ret := false
	if req.Req_type == "N" {
		fmt.Println("don't continue for ls")
		return true
	}
	file_lock.Lock()
	value, exists := file_locks[req.Sdfs_name]
	if !exists {
		file := common.CreateFile(req.Sdfs_name)
		file_locks[req.Sdfs_name] = file
		ret = true
	} else {
		if value.Readlock == false && value.Writelock == false {
			if strings.Contains(req.Sdfs_name, "juice_out/") && req.Command == "put" {
				fmt.Println("in here for putting this")
			}
			ret = true
		} else {
			if strings.Contains(req.Sdfs_name, "juice_out/") && req.Command == "put" {
				fmt.Println("readlock", value.Readlock, "writelock", value.Writelock)
			}
			ret = false
		}
	}
	// if strings.Contains(req.Sdfs_name, "juice_out/") && req.Command == "put" {
	// 	fmt.Println("putting to", req.Sdfs_name, "is", ret)
	// }
	file_lock.Unlock()
	return ret
}

func DeleteFileLocks(sdfs_name string) {
	file_lock.Lock()
	delete(file_locks, sdfs_name)
	file_lock.Unlock()
}

func UnlockFile(sdfs_name string, req_type string) {
	file_lock.Lock()
	file, exists := file_locks[sdfs_name]
	if exists {
		switch req_type {
		case "W":
			file.Writelock = false
		case "R":
			file.Readlock = false
			file.Numreaders -= 1
		}
	} else {
		fmt.Println("Cannot unlock file that does not exist")
	}
	file_lock.Unlock()
}

func LockFile(sdfs_name string, req_type string) {
	file_lock.Lock()
	file, exists := file_locks[sdfs_name]
	if exists {
		switch req_type {
		case "W":
			file.Writelock = true
		case "R":
			file.Readlock = true
			file.Numreaders += 1
		}
	} else {
		fmt.Println("Cannot lock file that does not exist")
	}
	file_lock.Unlock()
}

// helper method to find servers when calling the put request
func FindServersPut(n int, servers []string) []string {
	fc_lock.Lock()
	var kvSlice []struct {
		Key   string
		Value int
	}
	for k, v := range fcounts {
		kvSlice = append(kvSlice, struct {
			Key   string
			Value int
		}{k, v})
	}
	fc_lock.Unlock()
	// Sort the slice by values in ascending order.
	sort.Slice(kvSlice, func(i, j int) bool {
		return kvSlice[i].Value < kvSlice[j].Value
	})

	count := 0
	var ret_list []string
	for _, kv := range kvSlice {
		found := false
		for i := 0; i < len(servers); i++ {
			if kv.Key == servers[i] {
				found = true
			}
		}
		if !found {
			if count == n {
				break
			}
			// check server validity
			if common.IsServerAlive(kv.Key) {
				ret_list = append(ret_list, kv.Key)
				count += 1
			}

		}
	}
	return ret_list
}

// helper method to find available servers for get request
func FindServerGet(filename string, server string) []string {
	sf_lock.Lock()
	val, exists := sdfs_f[filename]
	sf_lock.Unlock()
	fc_lock.Lock()
	max_count := -1000
	var max_vm string
	// finds the servers the have least congestion
	if exists {
		for i := 0; i < len(val); i++ {
			if val[i] != server {
				count, e := fcounts[val[i]]
				if e {
					if count > max_count {
						max_count = count
						max_vm = val[i]
					}
				}
			}
		}
	}
	fc_lock.Unlock()
	return []string{max_vm}
}

// handles request objects and routes them
func HandleRequest(req *common.Request) {
	var resp *common.Response
	switch req.Command {
	case "put":
		// PrintFcount()
		// populate fcounts with mem_list
		// loop through the return val for GetMemList
		// if the string is not in fcounts add w 0
		var servers []string
		sf_lock.Lock()

		val, exists := sdfs_f[req.Sdfs_name]
		fmt.Println("val", val)
		sf_lock.Unlock()
		if exists {
			servers = val
		} else {
			servers = FindServersPut(common.MIN_REPS, []string{})
			UpdatePutRequests(servers, req.Sdfs_name)
			EditLineCounts("add", req.Sdfs_name, req.Line_count)
		}
		// PrintFcount()
		resp = common.GenResponse("put", servers, req.Sdfs_name, req.Local_name, req.Req_id)
		common.SendResponse(req.Id, common.FOLLOWER_RQ_PORT, resp)
		// fmt.Println("putting to:", servers)
	case "get":
		if FileExistsSdfs(req.Sdfs_name) {
			server := FindServerGet(req.Sdfs_name, req.Id)
			resp = common.GenResponse("get", server, req.Sdfs_name, req.Local_name, req.Req_id)
			common.SendResponse(req.Id, common.FOLLOWER_RQ_PORT, resp)
		}
	case "ls":
		var servers []string
		sf_lock.Lock()
		val, exists := sdfs_f[req.Sdfs_name]
		sf_lock.Unlock()
		if exists {
			servers = val
		}
		resp = common.GenResponse("ls", servers, req.Sdfs_name, req.Local_name, req.Req_id)
		common.SendResponse(req.Id, common.FOLLOWER_RQ_PORT, resp)
	case "delete":
		var servers []string
		sf_lock.Lock()
		val, exists := sdfs_f[req.Sdfs_name]
		sf_lock.Unlock()
		if exists {
			servers = val
		}
		UpdateDeleteRequests(servers, req.Sdfs_name)
		resp = common.GenResponse("delete", servers, req.Sdfs_name, req.Local_name, req.Req_id)
		common.SendResponse(req.Id, common.FOLLOWER_RQ_PORT, resp)
	case "update":
		fmt.Println("for updating leader data structures after a previous run has been killed")
	case "append":
		var servers []string
		sf_lock.Lock()
		val, exists := sdfs_f[req.Sdfs_name]
		sf_lock.Unlock()
		if exists {
			servers = val
		} else {
			servers = FindServersPut(common.MIN_REPS, []string{})
			UpdatePutRequests(servers, req.Sdfs_name)
			EditLineCounts("add", req.Sdfs_name, req.Line_count)
		}
		resp = common.GenResponse("append", servers, req.Sdfs_name, req.Local_name, req.Req_id)
		common.SendResponse(req.Id, common.FOLLOWER_RQ_PORT, resp)

	default:
		fmt.Println("Never gonna give you up! Never gonna let you down!")
	}
	// send response object back to leader
}

// deletes data associated with requests that need to be deleted
func UpdateDeleteRequests(servers []string, filename string) {
	for i := 0; i < len(servers); i++ {
		EditFCount("decrement", servers[i])
		EditSF("delete", filename, servers[i])
	}
	EditIPTQ("delete", filename, "")
	EditFWrites("delete", filename)
	EditFReads("delete", filename)
	EditLineCounts("delete", filename, 0)
	// DeleteFileLocks(filename)
}

// function to re-replicate files in case of vm failures
func RereplicateFiles(vm string) { // vm = failed vm
	var servers []string
	sf_lock.Lock()
	for file := range sdfs_f { // iterating over filenames
		val, exists := sdfs_f[file] // val = vm's that have a replica of that file
		if exists {
			// need to check server validity before doing the next part
			for i := 0; i < len(val); i++ {
				if vm == val[i] { // if this is true, that means that the failed vm stores this current file
					servers = FindServersPut(1, val)
					fmt.Println(servers)
					if len(servers) == 1 {
						// telling a node that contains the file to be replicated
						// to go replicate the file
						resp := common.GenResponse("load", servers, file, "", "")

						val = common.RemoveAtIndexStr(val, i)
						sdfs_f[file] = append(val, servers[0])
						send_to := val[0] // server to send response object to
						if !common.IsServerAlive(send_to) {
							for k := 0; k < len(val); k++ {
								if common.IsServerAlive(val[k]) {
									send_to = val[k]
									break
								}
							}
						}
						// TODO add a request to the in_prog_task_q about write to files
						// IF THE FILE IS LOCKED, WAIT FOR IT TO BE UNLOCKED AND THEN DO IT
						LockFile(resp.Sdfs_name, resp.Req_type)
						common.SendResponse(send_to, common.FOLLOWER_RQ_PORT, resp)
						EditFCount("add", servers[0])
						break
					} else {
						fmt.Println("Generated", len(servers), "servers")
					}

				}
			}
		}
	}
	sf_lock.Unlock()
}

// adds relevant info into the leader data structures regarding put requests
func UpdatePutRequests(servers []string, filename string) {
	for i := 0; i < len(servers); i++ {
		EditFCount("add", servers[i])
		EditSF("add", filename, servers[i])
	}
}

/**
* generic function to edit the file task queue
* acquires locks and unlocks inside
* can handle adding/modifying/delete operations regarding the queue
 */
func EditFileTaskQ(op string, req *common.Request) {
	ftq_lock.Lock()
	switch op {
	case "add":
		val, exists := file_task_q[req.Sdfs_name]
		if !exists {
			file_task_q[req.Sdfs_name] = []*common.Request{req}
		} else {
			file_task_q[req.Sdfs_name] = append(val, req)
		}
	case "rm":
		val, exists := file_task_q[req.Sdfs_name]
		if exists {
			for i := 0; i < len(val); i++ {
				if common.IsEqualRequest(req, val[i]) {
					file_task_q[req.Sdfs_name] = common.RemoveAtIndex(val, i)
					break
				}
			}
		}
	case "delete":
		delete(file_task_q, req.Sdfs_name)
	}
	ftq_lock.Unlock()
}

// removes tasks associated with failed vms
func RemoveFailedVMTasks(vm string) {
	ftq_lock.Lock()
	for file := range file_task_q {
		val, exists := file_task_q[file]
		if exists {
			for i := 0; i < len(val); i++ {
				if val[i].Id == vm {
					file_task_q[file] = common.RemoveAtIndex(val, i)
				}
			}
		}
	}
	ftq_lock.Unlock()
	otq_lock.Lock()
	for i := 0; i < len(overall_task_q); i++ {
		if overall_task_q[i].Id == vm {
			overall_task_q = common.RemoveAtIndex(overall_task_q, i)
		}
	}
	otq_lock.Unlock()
}

/**
* generic function to edit the overall task queue
* acquires locks and unlocks inside
* can handle adding/delete operations regarding the queue
 */
func EditOTaskQ(op string, req *common.Request) {
	otq_lock.Lock()
	switch op {
	case "add":
		overall_task_q = append(overall_task_q, req)
	case "delete":
		// fmt.Println("deleting from otq")
		for i := 0; i < len(overall_task_q); i++ {
			if common.IsEqualRequest(req, overall_task_q[i]) {
				// fmt.Println("going to delete this request")
				overall_task_q = common.RemoveAtIndex(overall_task_q, i)
				break
			}
		}
	}
	otq_lock.Unlock()
}

/**
* generic function to edit the sdfs file map
* acquires locks and unlocks inside
* can handle adding/delete operations regarding the queue
 */
func EditSF(op string, file string, vm string) {
	sf_lock.Lock()
	switch op {
	case "add":
		val, exists := sdfs_f[file]
		if !exists {
			sdfs_f[file] = []string{vm}
		} else {
			sdfs_f[file] = append(val, vm)
		}
		// fmt.Println("val after adding to dsfs_f", val)
	case "delete":
		delete(sdfs_f, file)
	}
	sf_lock.Unlock()
}

// function to remove from the sdfs file map
func RemoveSFVM(vm string) {
	sf_lock.Lock()
	for file := range sdfs_f {
		val, exists := sdfs_f[file]
		if exists {
			for i := 0; i < len(val); i++ {
				if val[i] == vm {
					sdfs_f[file] = common.RemoveAtIndexStr(val, i)
					break
				}
			}
		}
	}
	sf_lock.Unlock()
}

/**
* generic function to edit the fcount map
* acquires locks and unlocks inside
* can handle adding/modifying/delete operations regarding the queue
 */
func EditFCount(op string, vm string) {
	fc_lock.Lock()
	switch op {
	case "add":
		_, exists := fcounts[vm]
		if !exists {
			fcounts[vm] = 0
		} else {
			fcounts[vm] += 1
		}
	case "delete":
		delete(fcounts, vm)
	case "decrement":
		_, exists := fcounts[vm]
		if exists {
			fcounts[vm] -= 1
		}
	}
	fc_lock.Unlock()
}

func GetFCounts() []string {
	fc_lock.Lock()
	mems := make([]string, 0, len(fcounts))
	for key := range fcounts {
		mems = append(mems, key)
	}
	fc_lock.Unlock()
	return mems
}

func EditLineCounts(op string, file string, counts int) {
	lc_lock.Lock()
	switch op {
	case "add":
		line_counts[file] = counts
	case "delete":
		delete(line_counts, file)
	}
	lc_lock.Unlock()
}

/**
* generic function to edit the fwrites map
* acquires locks and unlocks inside
* can handle adding/delete/resetting operations regarding the queue
 */
func EditFWrites(op string, file string) {
	fw_lock.Lock()
	switch op {
	case "add":
		_, exists := file_writes[file]
		if !exists {
			file_writes[file] = 1
		} else {
			file_writes[file] += 1
		}
	case "reset":
		file_writes[file] = 0
	case "delete":
		delete(file_writes, file)
	}
	fw_lock.Unlock()
}

/**
* generic function to edit the fwrites map
* acquires locks and unlocks inside
* can handle adding/delete/resetting operations regarding the queue
 */
func EditFReads(op string, file string) {
	fr_lock.Lock()
	switch op {
	case "add":
		_, exists := file_reads[file]
		if !exists {
			file_reads[file] = 1
		} else {
			file_reads[file] += 1
		}
	case "reset":
		file_reads[file] = 0
	case "delete":
		delete(file_reads, file)
	}
	fr_lock.Unlock()
}

/**
* generic function to edit the In Progress Task Queue
* acquires locks and unlocks inside
* can handle adding/delete/removing operations regarding the queue
 */
func EditIPTQ(op string, file string, rq_type string) {
	ipq_lock.Lock()
	switch op {
	case "add":
		_, exists := in_progress_tq[file]
		if !exists {
			in_progress_tq[file] = make(map[string]int)
			in_progress_tq[file][rq_type] = 1
		} else {
			_, e2 := in_progress_tq[file][rq_type]
			if !e2 {
				in_progress_tq[file][rq_type] = 1
			} else {
				in_progress_tq[file][rq_type] += 1
			}
		}
	case "rm":
		_, exists := in_progress_tq[file][rq_type]
		if exists {
			in_progress_tq[file][rq_type] -= 1
		}
	case "delete":
		delete(in_progress_tq, file)
	}

	ipq_lock.Unlock()
}

// Helper methods for testing command functionality
func PrintFcount() {
	fc_lock.Lock()
	for vm := range fcounts {
		val, exists := fcounts[vm]
		if exists {
			fmt.Println("fcounts:", vm, val)
		}
	}
	fc_lock.Unlock()
}

// method ot print the sdfs map
func PrintSDFS() {
	sf_lock.Lock()
	for file := range sdfs_f {
		val, exists := sdfs_f[file]
		if exists {
			fmt.Println("sdfs:", file, val)
		}
	}
	sf_lock.Unlock()
}

func FileExistsSdfs(filename string) bool {
	sf_lock.Lock()
	_, exists := sdfs_f[filename]
	sf_lock.Unlock()
	return exists
}

func FindFilesForDir(dir string) map[string][]string {
	var output = make(map[string][]string)
	sf_lock.Lock()
	for file := range sdfs_f {
		val, exists := sdfs_f[file]
		if exists {
			if strings.Contains(file, dir) {
				output[file] = val
			}
		}
	}
	sf_lock.Unlock()
	return output
}

func GetLineCounts(file string) int {
	lc_lock.Lock()
	ret := line_counts[file]
	lc_lock.Unlock()
	return ret
}

func FindFilesForDirPref(dir string) []string {
	var output = []string{}
	sf_lock.Lock()
	for file := range sdfs_f {
		_, exits := sdfs_f[file]
		if exits {
			if strings.Contains(file, dir) {
				output = append(output, file)
			}
		}
	}
	sf_lock.Unlock()
	return output
}

// check if a request is valid
func validateRequest(req *common.Request) bool {
	if req.Command == "get" || req.Command == "delete" {
		if FileExistsSdfs(req.Sdfs_name) {
			return true
		} else {
			fmt.Println(req.Command, req.Sdfs_name)
			return false
		}
	}
	return true
}
