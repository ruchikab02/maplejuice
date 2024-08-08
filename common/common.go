package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// consts across program
const (
	GOSSIP_PORT      = "8080"
	TCP_PORT         = "8081"
	FOLLOWER_F_PORT  = "8082"
	LEADER_PORT      = "8083" // Listener for adding tasks to the queue
	FOLLOWER_RQ_PORT = "8084" // To communicate with the leader
	LEADER_MJ_PORT   = "8085"
	NODE_MJ_PORT     = "8086"
	STATUS_PORT      = "8087"
	GOSSIP_CONN_TYPE = "udp"
	JOIN_CONN_TYPE   = "tcp"
	FAILED           = "failed"
	RUNNING          = "running"
	SUS              = "suspicious"
	INTRODUCER       = "fa23-cs425-3401.cs.illinois.edu"
	SUSPICION_EN     = "SUSPICION_EN"
	SUSPICION_DIS    = "SUSPICION_DIS"
	TCLEANUP         = 4
	TFAIL            = 4
	TGOSSIP          = 1
	NUM_SERVERS      = 3
	BUF_SIZE         = 4096
	TSUS             = 4
	MIN_REPS         = 4
	NUM_LINES        = 30
)

// global map to determine what kind of request a command is
var Requests_map = map[string]string{
	"put":       "W",
	"get":       "R",
	"multiread": "R",
	"ls":        "N",
	"delete":    "W",
	"append":    "W",
	"load":      "W",
}

// a struct used for locking and unlocking files
type File struct {
	Filename   string
	Readlock   bool
	Writelock  bool
	Numreaders int
}

// object to hold a member in membership list
type Member struct {
	Id           string
	Status       string
	Hb_counter   int
	Timestamp    time.Time
	IsIntroducer bool
	Incarnation  int
}

// object to hold information about an incoming request from client
type Request struct {
	Id         string
	Req_type   string // read or write
	Command    string // get, put, delete, etc.
	Sdfs_name  string
	Local_name string
	IsFinished string
	Req_id     string
	Line_count int
}

// object to hold information about response being sent to followers from leader
type Response struct {
	Command    string
	Req_type   string
	Content    []string
	Sdfs_name  string
	Local_name string
	Req_id     string
}

// MJ request struct that gets sent to leader node
type MJ_Req struct {
	Id         string
	Type       string
	Exe        string
	Num_tasks  int
	Int_pref   string
	Src_dir    string // maple only
	Dest_name  string // juice only
	Del_input  int    // juice only
	IsFinished bool
	Input      string
}

// MJ response to store data sent to worker nodes
type MJ_Resp struct {
	Id        string
	Type      string
	Exe       string
	Num_tasks int
	Int_pref  string
	Src_dir   string
	Dest_name string
	Del_input int
	Files     []string
	Start     int
	End       int
	Input     string
}

// creates a file with appropriate locks
func CreateFile(filename string) *File {
	file := File{}
	file.Filename = filename
	file.Readlock = false
	file.Writelock = false
	file.Numreaders = 0
	return &file
}

// to create an mj response
func GenMJResp(id string, task string, exe string, num_tasks int, int_pref string, src_dir string, dest_name string, del_input int, files []string, start int, end int, input string) *MJ_Resp {
	mj_resp := MJ_Resp{}
	mj_resp.Id = id
	mj_resp.Type = task
	mj_resp.Exe = exe
	mj_resp.Num_tasks = num_tasks
	mj_resp.Int_pref = int_pref
	mj_resp.Src_dir = src_dir
	mj_resp.Dest_name = dest_name
	mj_resp.Del_input = del_input
	mj_resp.Files = files
	mj_resp.Start = start
	mj_resp.End = end
	mj_resp.Input = input
	return &mj_resp
}

// to create an mj request
func CreateMJReq(task string, exe string, num_tasks int, int_pref string, src_dir string, dest_name string, del_input int) *MJ_Req {
	mj_req := MJ_Req{}
	hostname, _ := os.Hostname()
	time := time.Now()
	mj_req.Id = string(hostname) + time.String()
	mj_req.Type = task
	mj_req.Exe = exe
	mj_req.Num_tasks = num_tasks
	mj_req.Int_pref = int_pref
	mj_req.Src_dir = src_dir
	mj_req.Dest_name = dest_name
	mj_req.Del_input = del_input
	mj_req.IsFinished = false
	mj_req.Input = ""
	return &mj_req
}

// helper function to generate a response from leader
func GenResponse(cmd string, cont []string, sdfs_name string, local_name string, req_id string) *Response {
	response := Response{}
	response.Command = cmd
	response.Req_type = Requests_map[cmd]
	response.Content = cont
	response.Sdfs_name = sdfs_name
	response.Local_name = local_name
	response.Req_id = req_id

	return &response
}

// helper function to create a request from a client
func CreateRequest(cmd string, sdfs_name string, local_name string, line_count int) *Request {
	newreq := Request{}
	hostname, _ := os.Hostname()
	time := time.Now()
	newreq.Id = string(hostname)
	typ, exists := Requests_map[cmd]
	if !exists {
		fmt.Println("Couldn't map cmd to type:", cmd)
	}
	newreq.Command = cmd
	newreq.Req_type = typ
	newreq.Sdfs_name = sdfs_name
	newreq.Local_name = local_name
	newreq.Req_id = string(hostname) + time.String()
	newreq.IsFinished = "false"
	newreq.Line_count = line_count
	return &newreq
}

// function to print the request in readable format
func PrintRequest(req *Request) {
	fmt.Println("Id", req.Id)
	fmt.Println("Req_type", req.Req_type)
	fmt.Println("Command", req.Command)
	fmt.Println("Sdfs_name", req.Sdfs_name)
	fmt.Println("Local_name", req.Local_name)
	fmt.Println("IsFinished", req.IsFinished)
	fmt.Println("Req_id", req.Req_id)
}

func PrintResponse(res *Response) {
	fmt.Println("Command", res.Command)
	fmt.Println("Content", res.Content)
	fmt.Println("Sdfs_name", res.Sdfs_name)
	fmt.Println("Local_name", res.Local_name)
	fmt.Println("Req_id", res.Req_id)
}

func PrintMJReq(mj_req *MJ_Req) {
	fmt.Println("Id", mj_req.Id)
	fmt.Println("Type", mj_req.Type)
	fmt.Println("Exe", mj_req.Exe)
	fmt.Println("Num_tasks", mj_req.Num_tasks)
	fmt.Println("Int_pref", mj_req.Int_pref)
	fmt.Println("Src_dir", mj_req.Src_dir)
	fmt.Println("Dest_name", mj_req.Dest_name)
	fmt.Println("Del_input", mj_req.Del_input)
	fmt.Println("IsFinished", mj_req.IsFinished)
}

func PrintMJResp(mj_resp *MJ_Resp) {
	fmt.Println("Id", mj_resp.Id)
	fmt.Println("Type", mj_resp.Type)
	fmt.Println("Exe", mj_resp.Exe)
	fmt.Println("Num_tasks", mj_resp.Num_tasks)
	fmt.Println("Int_pref", mj_resp.Int_pref)
	fmt.Println("Src_dir", mj_resp.Src_dir)
	fmt.Println("Dest_name", mj_resp.Dest_name)
	fmt.Println("Del_input", mj_resp.Del_input)
	fmt.Println("Start", mj_resp.Start)
	fmt.Println("End", mj_resp.End)
}

// gets the hostname given a vm + number
func GetHost(vm string) (string, error) {
	hostname, _ := os.Hostname()
	ret_id := string(hostname)
	vm = strings.TrimSpace(vm)
	if len(vm) < 3 {
		err := errors.New("Incorrect input for vm name")
		return "", err
	}
	if vm[0:2] != "vm" {
		fmt.Println("invalid name", vm[0:2])
		fmt.Println()
		err := errors.New("Not a vm name")
		return "", err
	}
	if len(vm) == 3 {
		vm_num := "0" + string([]rune(vm)[2])
		ret_id = ret_id[:13] + vm_num
		return ret_id, nil
	} else if len(vm) == 4 {
		if vm[2:4] != "10" {
			err := errors.New("Not a correct vm")
			return "", err
		}
		vm_num := vm[2:4]
		ret_id = ret_id[:13] + vm_num
		return ret_id + ".cs.illinois.edu", nil
	} else {
		err := errors.New("Incorrect vm name")
		return "", err
	}
}

// helper function to create a new member for membership list
func NewMember() *Member {
	// Obtain local information such as time and hostname
	time := time.Now()
	hostname, _ := os.Hostname()
	new_id := string(hostname) + time.String()
	newMem := Member{}
	newMem.Id = new_id
	newMem.Hb_counter = 0
	newMem.Timestamp = time
	newMem.Status = RUNNING
	newMem.IsIntroducer = string(hostname) == INTRODUCER
	newMem.Incarnation = 0
	return &newMem

}

// create the suspicion node
func SusGossip() *Member {
	sus_node := Member{}
	sus_node.Id = SUSPICION_EN
	sus_node.Hb_counter = 0
	sus_node.Timestamp = time.Now()
	sus_node.Status = RUNNING
	sus_node.IsIntroducer = false
	sus_node.Incarnation = 0
	return &sus_node
}

// helper function to calculate max
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// helper function to marshal a string list
func MarshalStrList(list []string) []byte {
	tmp := [][]byte{}
	for i := 0; i < len(list); i++ {
		output, err := json.Marshal(list[i])
		if err != nil {
			fmt.Println("marshaling error", err)
		}
		tmp = append(tmp, output)
	}

	ret_string, err1 := json.Marshal(tmp)
	if err1 != nil {
		fmt.Println("marshaling error", err1)
	}
	return ret_string
}

// helper function to unmarshal a string list
func UnmarshalStrList(response []byte) []string {
	var byte_list [][]byte

	err := json.Unmarshal(response, &byte_list)
	if err != nil {
		fmt.Println("unmarshaling error list", err)
	}

	var ret_list []string
	for i := 0; i < len(byte_list); i++ {
		var str string
		err1 := json.Unmarshal(byte_list[i], &str)
		if err1 != nil {
			fmt.Println("unmarshaling error string in strlist", err1)
		}
		ret_list = append(ret_list, str)
	}
	return ret_list
}

// TCP connection to send a request object
func SendRequest(id string, port string, req *Request) {
	conn, err := net.Dial(JOIN_CONN_TYPE, id+":"+port)
	if err != nil {
		fmt.Println("Could not connect to request machine:", err)
		conn.Close()
		return
	}
	// Write the serialized request to the leader
	marshaled, err := json.Marshal(req)
	_, err = conn.Write(marshaled)
	if err != nil {
		conn.Close()
		fmt.Println("Could not write object:", err)
		return
	}
	conn.Close()
}

func SendMJReq(id string, port string, mj_req *MJ_Req) {
	conn, err := net.Dial(JOIN_CONN_TYPE, id+":"+port)
	if err != nil {
		fmt.Println("Could not connect to MJ requests machine:", err)
		conn.Close()
		return
	}
	marshaled, err := json.Marshal(mj_req)
	_, err = conn.Write(marshaled)
	if err != nil {
		conn.Close()
		fmt.Println("Could not write object", err)
		return
	}
	conn.Close()
}

func SendMJResp(id string, port string, mj_resp *MJ_Resp) error {
	conn, err := net.Dial(JOIN_CONN_TYPE, id+":"+port)
	if err != nil {
		fmt.Println("Could not connect to MR responses machine:", err)
		conn.Close()
		return err
	}
	marshaled, err := json.Marshal(mj_resp)
	_, err = conn.Write(marshaled)
	if err != nil {
		conn.Close()
		fmt.Println("Could not write object", err)
		return err
	}
	conn.Close()
	return nil
}

func IsServerAlive(id string) bool {
	conn, err := net.DialTimeout(JOIN_CONN_TYPE, id+":"+STATUS_PORT, 1*time.Second)
	if err != nil {
		// fmt.Println("This machine is dead")
		return false
	}
	conn.Close()
	// fmt.Println("server is alive")
	return true
}

// TCP connection to send a response object
func SendResponse(id string, port string, resp *Response) {
	conn, err := net.Dial(JOIN_CONN_TYPE, id+":"+port)
	if err != nil {
		fmt.Println("Could not connect response machine:", err)
		fmt.Println("id", id, "port", port)
		PrintResponse(resp)
		conn.Close()
		return
	}
	// Write the serialized request to the leader
	// resp.Content = MarshalStrList(resp.Content)
	marshaled, err := json.Marshal(resp)
	_, err = conn.Write(marshaled)
	if err != nil {
		conn.Close()
		fmt.Println("Could not write object:", err)
		return
	}
	conn.Close()
}

// helper function to remove from a request list in place
func RemoveAtIndex(s []*Request, index int) []*Request {
	copy(s[index:], s[index+1:])
	return s[:len(s)-1]
}

// helper function to remove a string from a list in place
func RemoveAtIndexStr(s []string, index int) []string {
	copy(s[index:], s[index+1:])
	return s[:len(s)-1]
}

func RemoveElement(slice []string, value string) []string {
	var result []string
	for _, v := range slice {
		if v != value {
			result = append(result, v)
		}
	}
	return result
}

func RemoveAtIndexMJ(s []*MJ_Req, index int) []*MJ_Req {
	copy(s[index:], s[index+1:])
	return s[:len(s)-1]
}

// helper function to check if two request objects are for the same request
func IsEqualRequest(req1 *Request, req2 *Request) bool {
	return req1.Req_id == req2.Req_id
}

func IsEqualMJRequest(req1 *MJ_Req, req2 *MJ_Req) bool {
	return req1.Id == req2.Id
}

func IsEqualFile(f1 *File, f2 *File) bool {
	return f1.Filename == f2.Filename
}

func CountLines(filename string) (int, error) {
	cmd := exec.Command("wc", "-l", filename)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// fmt.Println("Error:", err)
		return 0, err
	}
	lineCountStr := strings.Fields(string(output))[0]
	lineCount, _ := strconv.Atoi(lineCountStr)
	return lineCount, nil
}

func ReadFromLine(file string, start int, end int, fileobj *os.File) (string, error) {
	if _, err := fileobj.Seek(0, 0); err != nil {
		return "", err
	}
	ret := ""
	scanner := bufio.NewScanner(fileobj)
	curr := 1
	for scanner.Scan() {
		if curr >= start && curr <= end {
			// fmt.Println(scanner.Text())
			ret += scanner.Text()
			if curr != end {
				ret += "\n"
			}

		}
		curr += 1
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return ret, nil
}
