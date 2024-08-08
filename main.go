package main

import (
	"bufio"
	"fmt"
	"maple_juice/common"
	"maple_juice/mem_list"
	"maple_juice/mj_protocol"
	mj "maple_juice/mj_protocol"
	rqs "maple_juice/requests"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {

	hostname, _ := os.Hostname()
	// introducer-specific functions
	if hostname == common.INTRODUCER {
		go rqs.OpenLeaderListener()
		go rqs.CheckQueue()
		go mj.OpenLeaderListener()
		go mj.CheckQueue()
	}
	// opens cli and listeners to receive conn objects
	go ListenToCommandLine()
	go rqs.OpenResponseListener()
	go rqs.OpenFileListener()
	go mj.OpenResponseListener()
	go mem_list.StatusChecker()
	mem_list.MaintainMemberList()
}

// cli listener to take in client input
func ListenToCommandLine() {
	var sus_node *common.Member
	query_num := 0
	for {
		// string parsing
		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		split_input := strings.Split(input, " ")
		// fmt.Println(len(split_input))
		if len(split_input) < 1 {
			fmt.Println("Please input a command")
		}
		for i := 0; i < len(split_input); i++ {
			split_input[i] = strings.TrimSpace(split_input[i])
		}
		if err != nil {
			fmt.Println("Failed to read input: ", err)
			os.Exit(1)
		}
		var req *common.Request
		var mj_req *common.MJ_Req
		switch split_input[0] {
		case "leave":
			os.Exit(1)
		case "list_mem":
			mem_list.OutputList()
		case "list_self":
			mem_list.OutputSelf()
		case "enable_sus":
			mem_list.EnableSus(sus_node)
		case "disable_sus":
			mem_list.DisableSus(sus_node)
		case "put":
			if len(split_input) != 3 {
				checkArgs(len(split_input), 3)
				continue
			}
			if !fileExistsLocally(split_input[1]) {
				fmt.Println("This file does not exist locally")
				continue
			}
			line_count, _ := common.CountLines("local/" + split_input[1])
			req = common.CreateRequest(split_input[0], split_input[2], split_input[1], line_count)
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		case "get":
			if len(split_input) != 3 {
				checkArgs(len(split_input), 3)
				continue
			}
			// if !rqs.FileExistsSdfs(split_input[1]) {
			// 	fmt.Println("File does not exist in file system")
			// 	continue
			// }
			req = common.CreateRequest(split_input[0], split_input[1], split_input[2], 0)
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		case "delete":
			if len(split_input) != 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			// if !rqs.FileExistsSdfs(split_input[1]) {
			// 	fmt.Println("File does not exist in file system")
			// 	continue
			// }
			req = common.CreateRequest(split_input[0], split_input[1], "", 0)
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		case "ls":
			if len(split_input) != 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			// if !rqs.FileExistsSdfs(split_input[1]) {
			// 	fmt.Println("File does not exist in file system")
			// 	continue
			// }
			req = common.CreateRequest(split_input[0], split_input[1], "", 0)
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		case "store":
			rqs.OutputStore()
		case "multiread":
			if len(split_input) < 3 {
				checkArgs(len(split_input), 3)
				continue
			}
			// if !rqs.FileExistsSdfs(split_input[1]) {
			// 	fmt.Println("File does not exist in file system")
			// 	continue
			// }
			// multiread sdfs_filename local_filename vmi, vmj, ...
			// send len(vms) worth of get requests and change the request id to be from vmi, vmj, etc.
			vms := split_input[3:]
			for i := 0; i < len(vms); i++ {
				req = common.CreateRequest("get", split_input[1], split_input[2], 0)
				req.Id, err = common.GetHost(vms[i])
				if err != nil {
					fmt.Println(err, "vmname:", vms[i])
				}
				common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
			}
			req = common.CreateRequest("get", split_input[1], split_input[2], 0)
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		case "file_size":
			if len(split_input) < 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			// format: file_size path_to_file
			size := checkFileSize(split_input[1])
			Mb := size / 1000000
			fmt.Println("Size of", split_input[1], "is", size, "bytes and", Mb, "MB")
		case "show_tail":
			if len(split_input) < 3 {
				checkArgs(len(split_input), 3)
				continue
			}
			// show_tail filename
			filename := split_input[1]
			num_lines := split_input[2]
			cmd := exec.Command("tail", "-n", num_lines, filename) // Display the last 10 lines, adjust as needed

			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				fmt.Println("Error:", err)
			}
			fmt.Println("")
		case "show_less":
			if len(split_input) < 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			filename := split_input[1]
			cmd := exec.Command("less", filename)

			cmd.Stdout = os.Stdout
			cmd.Stdin = os.Stdin
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				fmt.Println("Error:", err)
			}
		case "show_more":
			if len(split_input) < 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			// q to quit
			filename := split_input[1]
			cmd := exec.Command("more", filename)

			cmd.Stdout = os.Stdout
			cmd.Stdin = os.Stdin
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				fmt.Println("Error:", err)
			}
		case "mom":
			fmt.Println("your mom is not here")
		case "vmtasks":
			mj_protocol.PrintVmtasks()
		case "place_all":
			if len(split_input) != 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			inp_dir := split_input[1]
			dir, err := os.Open("local/" + inp_dir)
			if err != nil {
				fmt.Println("Error opening directory:", err)
				return
			}
			defer dir.Close()

			// Read the directory entries
			fileInfos, err := dir.Readdir(-1)
			if err != nil {
				fmt.Println("Error reading directory:", err)
				return
			}
			for _, fileInfo := range fileInfos {
				fmt.Println(fileInfo.Name())
				filename := fileInfo.Name()
				path := inp_dir + "/" + filename
				line_count, _ := common.CountLines("local/" + path)
				req = common.CreateRequest("put", path, path, line_count)
				common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
			}
		case "clear":
			if len(split_input) == 1 {
				checkArgs(len(split_input), 2)
				continue
			}
			err := clearDirectory(split_input[1])
			if err != nil {
				fmt.Println("error running clear")
			}
		case "demo_folders":
			necessary_dirs := []string{"local/int_pref", "local/juice_out", "local/test_int_pref", "local/demo", "local/idk",
				"sdfs/demo", "sdfs/test", "sdfs/juice_out", "sdfs/idk", "sdfs/int_pref"}
			for _, folder := range necessary_dirs {
				createDirectoryIfNotExists(folder)
			}
		case "demo_prep":
			folders_for_demo := []string{"local/int_pref", "local/juice_out", "local/test_int_pref",
				"sdfs/demo", "sdfs/test", "sdfs/juice_out", "sdfs/idk", "sdfs/int_pref"}
			for _, folder := range folders_for_demo {
				clearDirectory(folder)
			}

		case "maple":
			if len(split_input) != 5 {
				checkArgs(len(split_input), 5)
				continue
			}
			task := split_input[0]
			exe_file := split_input[1]
			num_maples, _ := strconv.Atoi(split_input[2])
			sdfs_name := split_input[3]
			sdfs_dir := split_input[4]
			mj_req = common.CreateMJReq(task, exe_file, num_maples, sdfs_name, sdfs_dir, "", -1)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req)
		case "juice":
			if len(split_input) != 6 {
				checkArgs(len(split_input), 6)
				continue
			}
			task := split_input[0]
			exe_file := split_input[1]
			num_juices, _ := strconv.Atoi(split_input[2])
			sdfs_name := split_input[3]
			sdfs_dest := split_input[4]
			delete_inp, _ := strconv.Atoi(split_input[5])
			fmt.Println("before sending juice")
			mj_req = common.CreateMJReq(task, exe_file, num_juices, sdfs_name, "", sdfs_dest, delete_inp)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req)
			fmt.Println("after sending juice")
		case "SELECT":
			if len(split_input) == 7 {
				if validJoin(split_input) {
					d1 := split_input[3] // to get rid of the comma

					d2 := split_input[4]
					cond := split_input[6]
					callJoins(d1, d2, cond, split_input[3])
				}
				continue
			}
			if len(split_input) != 6 {
				checkArgs(len(split_input), 6)
				continue
			}
			cols := split_input[1]
			dir := split_input[3]
			input := split_input[5]
			if cols != "ALL" || split_input[2] != "FROM" || split_input[4] != "WHERE" {
				fmt.Println("SQL Query should be in the form SELECT ALL FROM [dataset dir] WHERE [expression]")
				continue
			}
			numba := strconv.Itoa(query_num)
			mj_req = common.CreateMJReq("maple", "q1m.py", 4, "sqlr"+numba, dir, "", -1)
			mj_req.Input = input
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req)
			time.Sleep(time.Second * 8)
			juice_req := common.CreateMJReq("juice", "q1j.py", 10, "sqlr"+numba, "", "query1r"+numba, 1)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, juice_req)
			fmt.Println("idk yet")
			query_num += 1
		case "perc_decomp":
			// for test 1 in demo
			if len(split_input) != 2 {
				checkArgs(len(split_input), 2)
				continue
			}
			input := split_input[1]
			mj_req1 := common.CreateMJReq("maple", "demo_m1.py", 4, "taskm1", "demo", "", -1)
			mj_req1.Input = input
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req1)
			time.Sleep(time.Second * 15)
			juice_req1 := common.CreateMJReq("juice", "demo_j1.py", 10, "taskm1", "", "task_j1", 1)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, juice_req1)
			time.Sleep(time.Second * 15)
			// clearDirectory("local/juice_out")
			mj_req2 := common.CreateMJReq("maple", "demo_m2.py", 1, "taskm2", "juice_out", "", -1)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req2)
			time.Sleep(time.Second * 15)
			juice_req2 := common.CreateMJReq("juice", "demo_j2.py", 10, "taskm2", "", "task_j2", 1)
			common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, juice_req2)

		default:
			fmt.Println("Unsupported Server Command")
		}
	}
}

func checkArgs(given int, expected int) {
	fmt.Println("Incorrect number of arguments")
	fmt.Println("You provided", given, "and needed", expected)
}

func fileExistsLocally(filename string) bool {
	_, err := os.Stat("local/" + filename)
	if err != nil {
		return false
	} else {
		return true
	}
}

func checkFileSize(filename string) int64 {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		fmt.Println("stat error here")
	}
	file_size := fileInfo.Size()
	return file_size
}

func clearDirectory(directoryPath string) error {
	// Open the directory
	dir, err := os.Open(directoryPath)
	if err != nil {
		return err
	}
	defer dir.Close()

	// Read all the files in the directory
	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	// Remove each file in the directory
	for _, fileInfo := range fileInfos {
		filePath := filepath.Join(directoryPath, fileInfo.Name())
		err := os.RemoveAll(filePath)
		if err != nil {
			return err
		}
	}
	fmt.Println("directories cleared")
	return nil
}

func createDirectoryIfNotExists(directoryPath string) error {
	// Check if the directory already exists
	_, err := os.Stat(directoryPath)
	if os.IsNotExist(err) {
		// Create the directory if it doesn't exist
		err := os.MkdirAll(directoryPath, 0755)
		if err != nil {
			return err
		}
		fmt.Printf("Directory created: %s\n", directoryPath)
	} else if err != nil {
		// Some other error occurred
		return err
	}

	return nil
}

func callJoins(d1 string, d2 string, cond string, dir string) {
	// call comb_data.py here first
	fmt.Println(d1, d2, cond)
	mj_req := common.CreateMJReq("maple", "q1m.py", 4, "sqlj", dir, "", -1)
	mj_req.Input = cond
	common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, mj_req)
	time.Sleep(time.Second * 8)
	juice_req := common.CreateMJReq("juice", "q1j.py", 10, "sqlj", "", "query1j", 1)
	common.SendMJReq(common.INTRODUCER, common.LEADER_MJ_PORT, juice_req)

}

// method for joins
func validJoin(input []string) bool {
	cols := input[1]
	if cols != "ALL" || input[2] != "FROM" || input[5] != "WHERE" {
		fmt.Println("SQL Query should be in the form SELECT ALL FROM d1, d2 WHERE [expression (no spaces)]")
		return false
	} else {
		return true
	}

}
