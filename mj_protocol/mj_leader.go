package mj_protocol

import (
	"encoding/json"
	"fmt"
	"maple_juice/common"
	rqs "maple_juice/requests"
	"net"
	"os"
	"sync"
	"time"
)

var task_q []*common.MJ_Req
var in_prog_task *common.MJ_Req
var num_tasks = -1
var vm_tasks = make(map[string]*common.MJ_Resp)

var tq_lock sync.RWMutex
var nt_lock sync.RWMutex
var ipt_lock sync.RWMutex
var vmt_lock sync.RWMutex

// listener for mj requests
func OpenLeaderListener() {
	// Listens via TCP connection
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.LEADER_MJ_PORT)
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
		var req *common.MJ_Req
		rec := make([]byte, common.BUF_SIZE)
		bytes_read, err := conn.Read(rec)
		err = json.Unmarshal(rec[:bytes_read], &req)
		if err != nil {
			fmt.Println("unmarshaling error listener", err)
		}
		// if a request is not finished, add to queue
		if !req.IsFinished {
			EditTaskQ("add", req)
			fmt.Println("adding", req.Type, "to task q")
		} else {
			// if a request has finished, remove it and remove any associate file locks
			EditNT("delete", 0)
			nt_lock.Lock()
			if num_tasks == 0 {
				num_tasks = -1
				fmt.Println("removing", req.Type, "from task q")
				EditTaskQ("delete", req)
				if req.Type == "juice" && req.Del_input == 1 {
					fmt.Println("in the case to delete")
					DeleteInputFiles(req)
				}
				ClearVmtasks()
			}
			nt_lock.Unlock()
		}
		defer conn.Close()
	}
}

// periodically runs to check the task queue for maplejuice
func CheckQueue() {
	for {
		nt_lock.Lock()
		tasks := num_tasks
		nt_lock.Unlock()
		tq_lock.Lock()
		var req *common.MJ_Req
		if len(task_q) != 0 && tasks == -1 {
			req = task_q[0]
		}
		tq_lock.Unlock()
		ipt_lock.Lock()
		if req != nil {
			if in_prog_task != nil && !common.IsEqualMJRequest(req, in_prog_task) {
				in_prog_task = req
				HandleMJRequest(req)
			} else if in_prog_task == nil {
				in_prog_task = req
				HandleMJRequest(req)
			}
		}
		ipt_lock.Unlock()
		// PrintVmtasks()
		time.Sleep(time.Second * time.Duration(common.TGOSSIP))
	}
}

// maplejuice request handler
func HandleMJRequest(req *common.MJ_Req) {
	var resp *common.MJ_Resp
	switch req.Type {
	case "maple":
		fmt.Println("Handling maple")

		all_files := rqs.FindFilesForDir(req.Src_dir + "/")
		for file := range all_files {
			val, exists := all_files[file]
			// figuring out how to allocate lines to different vm's
			if exists {
				lines := rqs.GetLineCounts(file)
				num_vms := len(val)
				num_threads := req.Num_tasks / num_vms
				end := 1
				start := 1
				size := lines / num_vms
				EditNT("add", num_vms)
				for i := 0; i < num_vms; i++ {
					end += size
					if i == num_vms-1 || end > lines {
						end = lines
					}
					// Generate response here
					files := []string{file}
					resp = common.GenMJResp(req.Id, req.Type, req.Exe, num_threads, req.Int_pref, req.Src_dir, req.Dest_name, req.Del_input, files, start, end, req.Input)
					common.PrintMJResp(resp)

					send_vm := val[i]
					err := common.SendMJResp(send_vm, common.NODE_MJ_PORT, resp)
					if err != nil {
						// resend the request to someone else that has the replica
						// get the list again to get updated info
						fmt.Println("send maple failed")
					}

					// by now we know that we have sent the response to the correct person
					AddToVmtasks(resp, send_vm)
					start = end + 1
				}
			}
		}

	case "juice":
		// NEW JUICE INCOMING...
		fmt.Println("received a juice command")
		all_files := rqs.FindFilesForDirPref("int_pref/" + req.Int_pref)
		num_keys := len(all_files) // number of keys we need to distribute over num_juices
		num_juices := req.Num_tasks
		if num_juices == 0 {
			fmt.Println("num juices is 0")
		}
		mems := rqs.GetFCounts()
		if len(mems) < req.Num_tasks {
			num_juices = len(mems)
		}
		if num_keys < num_juices {
			num_juices = num_keys
		}
		// how to allocate keys per juice task
		start := 0
		step := (num_keys - (num_keys % num_juices)) / num_juices
		end := step
		i := 0
		for start <= end {
			if start >= end {
				fmt.Println("if we are here go is dumb")
				break
			}
			if i >= len(mems) {
				i = 0
			}
			eof := false
			if end >= len(all_files) {
				end = len(all_files)
				eof = true
			}
			assigned := all_files[start:end]
			resp = common.GenMJResp(req.Id, req.Type, req.Exe, 1, req.Int_pref, req.Src_dir, req.Dest_name, req.Del_input, assigned, 0, 0, req.Input)
			send_vm := mems[i]
			err := common.SendMJResp(send_vm, common.NODE_MJ_PORT, resp)
			fmt.Println("sending response")
			EditNT("add", 1)
			if err != nil {
				// for failure detection
				// edit resp object
				// resend the request to someone else that has the replica
				fmt.Println("send juice failed")
			}
			// by now we know we have sent the response to the correct person, so add to map
			fmt.Println("adding to vmtasks")
			AddToVmtasks(resp, send_vm)

			i += 1
			start += step
			if !eof {
				end = start + step
			}
			fmt.Println("at end of loop start:", start, "end:", end)
		}
	}
}

// to handle delete_input = 1
func DeleteInputFiles(req *common.MJ_Req) {
	all_files := rqs.FindFilesForDirPref("int_pref/" + req.Int_pref)
	for _, filename := range all_files {
		path := filename
		fmt.Println(path)
		del_req := common.CreateRequest("delete", path, "", 0)
		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, del_req)
	}

}

/**
* A helper method to lock and unlock and make operations on the task queue
 */
func EditTaskQ(op string, req *common.MJ_Req) {
	tq_lock.Lock()
	switch op {
	case "add":
		task_q = append(task_q, req)
	case "delete":
		for i := 0; i < len(task_q); i++ {
			if common.IsEqualMJRequest(req, task_q[i]) {
				task_q = common.RemoveAtIndexMJ(task_q, i)
				break
			}
		}
	}
	tq_lock.Unlock()
}

/**
* A helper method to lock and unlock and make operations on num_tasks
 */
func EditNT(op string, num_vms int) {
	nt_lock.Lock()
	switch op {
	case "add":
		if num_tasks == -1 {
			num_tasks = num_vms
		} else {
			// fmt.Println("u probably shudnnt be here tbh")
			num_tasks += num_vms
		}
	case "delete":
		num_tasks -= 1
		fmt.Println("NUM:", num_tasks)
	}
	nt_lock.Unlock()
}

func AddToVmtasks(resp *common.MJ_Resp, vm string) {
	vmt_lock.Lock()
	vm_tasks[vm] = resp
	fmt.Println("adding to vmtasks", vm)
	vmt_lock.Unlock()
}

func ClearVmtasks() {
	vmt_lock.Lock()
	vm_tasks = make(map[string]*common.MJ_Resp)
	vmt_lock.Unlock()
}

func PrintVmtasks() {
	vmt_lock.Lock()
	for key, value := range vm_tasks {
		fmt.Println("vm:", key, "keys:", value.Files)
	}
	vmt_lock.Unlock()
}
