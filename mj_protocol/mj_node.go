package mj_protocol

import (
	"encoding/json"
	"fmt"
	"maple_juice/common"

	// rqs "maple_juice/requests"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// listener for mj tasks
func OpenResponseListener() {
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.NODE_MJ_PORT)
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
			continue
		}
		// receive response object
		var resp *common.MJ_Resp
		rec := make([]byte, common.BUF_SIZE)
		bytes_read, err := conn.Read(rec)
		if bytes_read == 0 {
			continue
		}
		err = json.Unmarshal(rec[:bytes_read], &resp)
		if err != nil {
			fmt.Println("unmarshaling error response listener", err)
			conn.Close()
			return
		}
		handleMJResponse(resp)
		conn.Close()
	}
}

func handleMJResponse(resp *common.MJ_Resp) {
	switch resp.Type {
	case "maple":
		lines := ((resp.End - resp.Start) + 1) / resp.Num_tasks
		start := resp.Start
		end := resp.Start
		// divide tasks across vms with replicas
		if len(resp.Files) == 0 {
			fmt.Println("received 0 keys for maple task")
			req := common.CreateMJReq("maple", resp.Exe, resp.Num_tasks, resp.Int_pref, resp.Src_dir, resp.Dest_name, resp.Del_input)
			req.IsFinished = true
			req.Id = resp.Id
			// send requests to alive servers
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, req)
			return
		}
		fp, err := os.Open("sdfs/" + resp.Files[0])
		if err != nil {
			fmt.Println("Could not open file:", err)
		}
		defer fp.Close()
		var wg sync.WaitGroup
		wg.Add(resp.Num_tasks)
		for i := 0; i < resp.Num_tasks; i++ {
			end += lines
			if i == resp.Num_tasks-1 || end > resp.End {
				end = resp.End
			}
			fmt.Println(i, "rfl start:", start, "end:", end)
			out, err := common.ReadFromLine("sdfs/"+resp.Files[0], start, end, fp)
			if err != nil {
				fmt.Println("error!", err)
			}
			// sends maple request to containers to run on
			go divideTasksMaple(out, resp.Exe, resp.Int_pref, &wg, resp.Input)
			start = end + 1
		}
		wg.Wait()
		dir, err := os.Open("local/int_pref")
		if err != nil {
			fmt.Println("Error opening local directory:", err)
		}
		fileInfos, err := dir.Readdir(-1)
		if err != nil {
			fmt.Println("Error reading local directory:", err)
		}
		// intermediate output files
		for _, fileInfo := range fileInfos {
			if strings.Contains(fileInfo.Name(), resp.Int_pref) {
				line_count, _ := common.CountLines(fileInfo.Name())
				req := common.CreateRequest("append", "int_pref/"+fileInfo.Name(), "int_pref/"+fileInfo.Name(), line_count)
				common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
			}
		}
		dir.Close()
		req := common.CreateMJReq("maple", resp.Exe, resp.Num_tasks, resp.Int_pref, resp.Src_dir, resp.Dest_name, resp.Del_input)
		req.IsFinished = true
		req.Id = resp.Id
		common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, req)
	case "juice":
		fmt.Println("handling juice")
		// divide num_keys across goroutines
		if len(resp.Files) == 0 {
			fmt.Println("no keys provided!")
			req := common.CreateMJReq("juice", resp.Exe, resp.Num_tasks, resp.Int_pref, resp.Src_dir, resp.Dest_name, resp.Del_input)
			req.IsFinished = true
			req.Id = resp.Id
			// sending juice responses to valid servers
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, req)
			return
		}
		result := strings.Split(resp.Files[0], "_")
		result2 := strings.Split(result[len(result)-1], ".")
		uid := result2[0]
		var wg sync.WaitGroup
		wg.Add(resp.Num_tasks)
		for i := 0; i < resp.Num_tasks; i++ {
			go divideTasksJuice(resp.Files, resp.Exe, resp.Dest_name+uid, &wg)
		}
		wg.Wait()
		for {
			// check if we have gotten the file
			_, err := os.Stat("local/juice_out/" + resp.Dest_name + uid + ".txt")
			if err != nil {
				time.Sleep(time.Duration(1) * time.Second)
			} else {
				fmt.Println("file exists locally b4 running python")
				break
			}
		}
		// output file to store juice on
		line_count, _ := common.CountLines("local/juice_out/" + resp.Dest_name + uid + ".txt")
		req1 := common.CreateRequest("append", "juice_out/"+resp.Dest_name+".txt", "juice_out/"+resp.Dest_name+uid+".txt", line_count)
		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req1)
		req := common.CreateMJReq("juice", resp.Exe, resp.Num_tasks, resp.Int_pref, resp.Src_dir, resp.Dest_name, resp.Del_input)
		req.IsFinished = true
		req.Id = resp.Id
		common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, req)
	}

}

// helper method that helps divide a file into chunks to send to maple tasks
func divideTasksMaple(cont string, exe string, int_pref string, wg *sync.WaitGroup, input string) {
	defer wg.Done()
	lines := strings.Split(cont, "\n")
	start := 0
	for start < len(lines) {
		end := common.Min(len(lines), start+common.NUM_LINES)
		to_parse := ""
		for i := start; i < end; i++ {
			to_parse += lines[i] + "\n"
		}
		fmt.Println("start", start, "end", end)
		executeMaple(to_parse, exe, int_pref, input)
		start = end
	}

}

// helper method that executes a juice command per key
func divideTasksJuice(keys []string, exe string, output string, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < len(keys); i++ {
		executeJuice(keys[i], exe, output)
	}
}

// actual maple task
func executeMaple(cont string, exe string, int_pref string, input string) {
	fmt.Println("executing maple")
	// call get for ALL files that have int_pref as substring
	path := "sdfs/exes/" + exe
	cmd := exec.Command("python3", path, cont, int_pref, input)
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error running Python script:", err)
		fmt.Println(err.Error())
	}
}

// actual juice task
func executeJuice(file string, exe string, output string) {
	fmt.Println("assigned", file)
	req1 := common.CreateRequest("get", file, "test_"+file, 0)
	common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req1)
	path := "sdfs/exes/" + exe
	for {
		// check if we have gotten the file
		_, err := os.Stat("local/" + "test_" + file)
		if err != nil {
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			fmt.Println("file exists locally b4 running python")
			break
		}
	}
	fmt.Println("python3", path, "test_"+file, output)
	cmd := exec.Command("python3", path, "test_"+file, output)
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error running Python script:", err)
	}
	fmt.Println("finished python")
}
