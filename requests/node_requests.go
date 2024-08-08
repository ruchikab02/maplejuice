package rqs

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maple_juice/common"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// map of local and sdfs files
var sdfs_files []string
var sf_vm_lock sync.RWMutex

// listener for response objects
func OpenResponseListener() {
	// Listens via TCP connection
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.FOLLOWER_RQ_PORT)
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
		var resp *common.Response
		rec := make([]byte, common.BUF_SIZE)
		bytes_read, err := conn.Read(rec)
		if bytes_read == 0 {
			continue
		}
		err = json.Unmarshal(rec[:bytes_read], &resp)
		if err != nil {
			fmt.Println("unmarshaling error response listener", err)
			os.Exit(1)
		}
		handleResponse(resp)
		// fmt.Println("Action completed:", resp.Req_id, resp.Content, resp.Command)
		defer conn.Close()
	}
}

// listener for receiving files
func OpenFileListener() {
	// Listens via TCP connection
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.FOLLOWER_F_PORT)
	if err != nil {
		fmt.Println("Could not connect to server:", err)
	}
	defer int_listener.Close()
	// accept a request
	for {
		conn, err := int_listener.Accept()
		if err != nil {
			fmt.Println("Could not accept a request:", err)
			return
		}
		defer conn.Close()

		// Receive num bytes for file name and request type
		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Read num bytes error:", err)
		}

		header := string(buf[:n])
		// request type
		req := string([]rune(header)[0])
		num_bytes_fn, _ := strconv.Atoi(header[1:4])

		// Receive the filename
		buf = make([]byte, num_bytes_fn)
		bytes_read, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Read filename error:", err)
		}
		filename := string(buf[:bytes_read])
		if req == "G" {
			// write your contents to the conn object
			writeFiletoConn("sdfs", filename, conn)
		} else if req == "D" {
			deleteFile("sdfs", filename)
			editStore("delete", filename)

		} else if req == "A" {
			editStore("add", filename)
			// open a new file on my machnine in the sdfs folder and replicate contents
			appendFileFromConn("sdfs", filename, conn)
		} else if req == "P" {
			editStore("add", filename)
			// open a new file on my machnine in the sdfs folder and replicate contents
			writeFilefromConn("sdfs", filename, conn)
		}

	}

}

// function to delete file
func deleteFile(dir string, filename string) {
	err := os.Remove(dir + "/" + filename)
	if err != nil {
		fmt.Println("could not delete file", err)
	}
}

// writes file contents from a conn object to a filename in directory dir (overwrites this)
func writeFilefromConn(dir string, filename string, conn net.Conn) {
	var size int64
	binary.Read(conn, binary.LittleEndian, &size)
	os.MkdirAll(dir, os.ModePerm) // Create the directory if it doesn't exist
	fp := filepath.Join(dir, filename)
	file, err := os.Create(fp)
	if err != nil {
		fmt.Println("Could not create file:", err)
		fmt.Println(fp)
		conn.Close()
		return
	}
	defer file.Close()

	// Receive and save file data
	_, err = io.CopyN(file, conn, size)
	if err != nil {
		fmt.Println("Could not copy to file", err)
		conn.Close()
		return
	}
	conn.Close()
}

// helper method to append to a file from a connection
func appendFileFromConn(dir string, filename string, conn net.Conn) {
	var size int64
	binary.Read(conn, binary.LittleEndian, &size)
	os.MkdirAll(dir, os.ModePerm) // Create the directory if it doesn't exist
	fp := filepath.Join(dir, filename)
	file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Receive and save file data
	_, err = io.CopyN(file, conn, size)
	if err != nil {
		fmt.Println("Could not copy to file", err)
		conn.Close()
		return
	}
	conn.Close()
}

// returns file size for a specified file
func getFileSize(filename string) int64 {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		fmt.Println(filename)
		fmt.Println("stat error", err)
		return -1
	}
	file_size := fileInfo.Size()
	return file_size
}

// wirtes a file (send_file) in dir (loc) to an open conn
func writeFiletoConn(loc string, send_file string, conn net.Conn) {
	size := getFileSize(loc + "/" + send_file)

	binary.Write(conn, binary.LittleEndian, size)

	file, err := os.Open(loc + "/" + send_file)
	if err != nil {
		fmt.Println("could not open file")
		os.Exit(1)
	}

	// Send actual file
	_, err = io.CopyN(conn, file, size)
	if err != nil {
		fmt.Println("Could not write file contents to connection", err)
		conn.Close()
		return
	}

	defer file.Close()

}

// sends file across to another node
func sendFile(local_file string, sdfs_file string, dest string, loc string, req string) {
	conn, err := net.Dial(common.JOIN_CONN_TYPE, dest+":"+common.FOLLOWER_F_PORT)
	if err != nil {
		fmt.Println("Could not connect to other node to send file:", err)
		fmt.Println("dest", dest)
		conn.Close()
		return
	}
	defer conn.Close()

	// Send the file name bytes with request type
	// format the length of sdfs file
	length := strconv.Itoa(len(sdfs_file))
	if len(length) == 0 {
		fmt.Println("empty string")
	}
	if len(length) == 2 {
		length = "0" + length
	} else if len(length) == 1 {
		length = "00" + length
	}
	header := req + length
	_, err = conn.Write([]byte(header))
	if err != nil {
		fmt.Println("Could not write file bytes to connection", err)
		conn.Close()
		return
	}

	// Send the filename
	_, err = conn.Write([]byte(sdfs_file))
	if err != nil {
		fmt.Println("Could not write file name to connection", err)
		conn.Close()
		return
	}

	if req == "G" {
		writeFilefromConn(loc, local_file, conn)
	} else if req == "P" {
		writeFiletoConn(loc, local_file, conn)
	} else if req == "D" {
	} else if req == "A" {
		writeFiletoConn(loc, local_file, conn)
	}

}

// response handler to route response objects
func handleResponse(resp *common.Response) {
	// common.PrintResponse(resp)
	switch resp.Command {
	case "put":
		// servers to put to
		servers := resp.Content
		for i := 0; i < len(servers); i++ {
			sendFile(resp.Local_name, resp.Sdfs_name, servers[i], "local", "P")
		}
		req := common.CreateRequest(resp.Command, resp.Sdfs_name, resp.Local_name, 0)
		req.Req_id = resp.Req_id
		req.IsFinished = "true"
		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		fmt.Println("sent finished put request back to leader")
	case "get":
		servers := resp.Content
		if len(servers) != 1 {
			fmt.Println("incorrect number of servers returned")
		} else {
			// sends server to get from
			if len(resp.Sdfs_name) == 0 {
				fmt.Println("another empty string")
			}
			sendFile(resp.Local_name, resp.Sdfs_name, servers[0], "local", "G")
			req := common.CreateRequest(resp.Command, resp.Sdfs_name, resp.Local_name, 0)
			req.Req_id = resp.Req_id
			req.IsFinished = "true"
			common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
		}
	case "ls":
		servers := resp.Content
		for i := 0; i < len(servers); i++ {
			fmt.Println(servers[i])
		}
	case "delete":
		// servers we're deleting from
		servers := resp.Content
		for i := 0; i < len(servers); i++ {
			sendFile(resp.Local_name, resp.Sdfs_name, servers[i], "", "D")
		}
		req := common.CreateRequest(resp.Command, resp.Sdfs_name, resp.Local_name, 0)
		req.Req_id = resp.Req_id
		req.IsFinished = "true"
		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
	case "load":
		// check if request can be executed
		servers := resp.Content
		for i := 0; i < len(servers); i++ {
			sendFile(resp.Sdfs_name, resp.Sdfs_name, servers[i], "sdfs", "P")
		}

		// TODO SEND A REQUEST BACK TO REMOVE THE FILE LOCK
		req := common.CreateRequest(resp.Command, resp.Sdfs_name, resp.Sdfs_name, 0)
		req.Req_id = resp.Req_id
		req.IsFinished = "true"
		req.Req_type = common.Requests_map[resp.Command]

		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)
	case "append":
		// servers to put to
		servers := resp.Content
		for i := 0; i < len(servers); i++ {
			sendFile(resp.Local_name, resp.Sdfs_name, servers[i], "local", "A")
		}
		req := common.CreateRequest(resp.Command, resp.Sdfs_name, resp.Local_name, 0)
		req.Req_id = resp.Req_id
		req.IsFinished = "true"
		common.SendRequest(common.INTRODUCER, common.LEADER_PORT, req)

	}
}

// function to output store command
func OutputStore() {
	sf_vm_lock.Lock()
	for i := 0; i < len(sdfs_files); i++ {
		fmt.Println(sdfs_files[i])
	}
	sf_vm_lock.Unlock()
}

// generic function to edit the store data structure
func editStore(op string, filename string) {
	sf_vm_lock.Lock()
	switch op {
	case "add":
		sdfs_files = append(sdfs_files, filename)
	case "delete":
		for i := 0; i < len(sdfs_files); i++ {
			sdfs_files = common.RemoveAtIndexStr(sdfs_files, i)
		}
	}
	sf_vm_lock.Unlock()
}
