package mem_list

import (
	"encoding/json"
	"fmt"
	"maple_juice/common"
	rqs "maple_juice/requests"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

// global vars
var (
	member_list    []*common.Member
	mutex          sync.RWMutex
	host           string
	sus_en         bool
	sus_mutex      sync.RWMutex
	sus_node_mutex sync.RWMutex
)

// enable the suspicion node to be passed around in membership list
func EnableSus(node *common.Member) {

	sus_mutex.Lock()
	sus_en = true
	sus_mutex.Unlock()
	sus_node_mutex.Lock()
	node = common.SusGossip()
	sus_node_mutex.Unlock()
	mutex.Lock()
	member_list = append(member_list, node)
	mutex.Unlock()
}

// disable the suspicion node to be marked as failed in membership list
func DisableSus(node *common.Member) {
	sus_mutex.Lock()
	sus_en = false
	sus_mutex.Unlock()
	sus_node_mutex.Lock()
	isNil := node == nil
	sus_node_mutex.Unlock()
	if !isNil {
		sus_node_mutex.Lock()
		node.Status = common.FAILED
		sus_node_mutex.Unlock()
	} else {
		mutex.Lock()
		for i := 0; i < len(member_list); i++ {
			if member_list[i].Id == common.SUSPICION_EN {
				member_list[i].Status = common.FAILED
			}
		}
		mutex.Unlock()
	}
}

// helper for list_mem
func OutputList() {
	mutex.Lock()
	for i := 0; i < len(member_list); i++ {
		fmt.Println(member_list[i].Id)
	}
	mutex.Unlock()
}

// helper for list_self
func OutputSelf() {
	hostname, _ := os.Hostname()
	mutex.Lock()
	for i := 0; i < len(member_list); i++ {
		if strings.Contains(member_list[i].Id, hostname) {
			fmt.Println(member_list[i].Id)
		}
	}
	mutex.Unlock()
}

// function to run in main.go to maintain the membership list
func MaintainMemberList() {
	h, _ := os.Hostname()
	host = string(h)
	cur_mem := common.NewMember()
	member_list = []*common.Member{cur_mem}

	// introducer/leader functionality when joining
	if cur_mem.IsIntroducer {
		rqs.EditFCount("add", cur_mem.Id[:31])
		// rqs.PrintFcount()
		fmt.Println("updated mem list")
		go joinListener()
	}

	// start up udp servers for other nodes
	go buildNode(cur_mem)
	sus_mutex.Lock()
	isSus := sus_en
	sus_mutex.Unlock()

	// check membership list periodically
	if isSus {
		go checkListSuspicion()
	} else {
		go checkList()
	}

	// gossip every second
	for {
		gossip(cur_mem)
		time.Sleep(time.Second * time.Duration(common.TGOSSIP))
	}
}

// helper function to marshal a membership list
func marshaler(mem_list []*common.Member) ([]byte, error) {
	// var output byte nested array
	int_list := [][]byte{}

	// Loop through all members in the member list and marshal their fields
	for i := 0; i < len(mem_list); i++ {
		output, err := json.Marshal(mem_list[i])
		if err != nil {
			return nil, err
		}

		// Append to nested byte array
		int_list = append(int_list, output)
	}

	// Marshal the whole member list
	ret_string, err1 := json.Marshal(int_list)
	if err1 != nil {
		return nil, err1
	}
	return ret_string, nil
}

// helper function to unmarshal a membership list
func unmarshaler(serial []byte) ([]*common.Member, error) {
	// Nested byte array to read into
	var int_list [][]byte

	// Unmarshal the byte array sent over
	err := json.Unmarshal(serial, &int_list)
	if err != nil {
		return nil, err
	}

	// Initialize an empty list of members to unmarshal into
	var mem_list []*common.Member

	// Unmarshal each field of each member objects
	for i := 0; i < len(int_list); i++ {
		// Initialize empty member object to read into
		var mem *common.Member
		err1 := json.Unmarshal(int_list[i], &mem)
		if err1 != nil {
			return nil, err1
		}

		// Populate list of members
		mem_list = append(mem_list, mem)
	}
	return mem_list, nil
}

func buildNode(cur_mem *common.Member) {
	// Clear the sdfs folder
	// directoryToClear := "sdfs"

	// err := os.RemoveAll(directoryToClear)
	// if err != nil {
	// 	panic(err)
	// }

	// println("Contents of", directoryToClear, "cleared successfully.")

	// Call the introducer using TCP connection
	if !cur_mem.IsIntroducer {
		conn, err := net.Dial(common.JOIN_CONN_TYPE, common.INTRODUCER+":"+common.TCP_PORT)
		if err != nil {
			fmt.Println("Could not connect to introducer:", err)
			conn.Close()
			return
		}

		// Send personal member list
		mutex.Lock()
		serialized_mems, err := marshaler(member_list)
		mutex.Unlock()
		if err != nil {
			conn.Close()
			fmt.Println("Error while serializing:", err)
			return
		}

		// Write the serialized member list to the introducer
		_, err = conn.Write(serialized_mems)
		if err != nil {
			conn.Close()
			fmt.Println("Could not write membership list to introducer:", err)
			return
		}

		// Read the introducer's member list being sent back
		int_serialized_list := make([]byte, common.BUF_SIZE)
		bytes_read, err := conn.Read(int_serialized_list)
		if err != nil {
			fmt.Println("Cannot read from introducer", err)
		}

		// Unmarshal the list of members
		mutex.Lock()
		inc_mem_list, err := unmarshaler(int_serialized_list[:bytes_read])
		mutex.Unlock()
		if err != nil {
			conn.Close()
			fmt.Println("Error while deserializing:", err)
			return
		}

		// Loop through the incoming membership list from introducer
		var tmp bool
		mutex.Lock()
		for i := 0; i < len(inc_mem_list); i++ {
			// Update timestamp of member being added to own list
			newMem := inc_mem_list[i]
			newMem.Timestamp = time.Now()
			if newMem.Id == common.SUSPICION_EN {
				tmp = true
			}

			// Update own member list
			member_list = append(member_list, newMem)
		}
		mutex.Unlock()
		if tmp == true {
			sus_mutex.Lock()
			sus_en = tmp
			sus_mutex.Unlock()
		}
		conn.Close()
	}
	// Begin listening for connections from other nodes/introducer using UDP
	udpServer, err := net.ListenPacket(common.GOSSIP_CONN_TYPE, ":"+common.GOSSIP_PORT)
	if err != nil {
		fmt.Println("Couldn't listen:", err)
	}
	defer udpServer.Close()

	// Infinite loop to listen for messages from other nodes/introducers
	for {
		// Receive Client message
		buffer := make([]byte, common.BUF_SIZE)
		bytes_read, _, err := udpServer.ReadFrom(buffer)
		if err != nil {
			fmt.Println("Could not receive membership list:", err)
		}
		sus_mutex.Lock()
		isSus := sus_en
		sus_mutex.Unlock()
		if isSus {
			updateMemberListSuspicion(buffer, bytes_read)
		} else {
			updateMemberList(buffer, bytes_read)
		}
	}
}

// start up TCP connection for introducer to listen for new members
func joinListener() {
	// Listens via TCP connection
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.TCP_PORT)
	if err != nil {
		fmt.Println("Could not connect to server:", err)
		os.Exit(1)
	}
	defer int_listener.Close()
	for {
		conn, err := int_listener.Accept()
		if err != nil {
			fmt.Println("Could not accept a request:", err)
			return
		}
		mutex.Lock()
		send_list, err := marshaler(member_list)
		mutex.Unlock()
		if err != nil {
			fmt.Println("couldn't marshal the list")
			os.Exit(1)
		}
		conn.Write(send_list)
		addMemberToList(conn)
	}
}

func updateMemberList(buffer []byte, bytes_read int) {
	// Unmarshal input from other nodes/introducer
	node_mem_list, err := unmarshaler(buffer[:bytes_read])
	if err != nil {
		fmt.Println("Could not deserialize membership list:", err)
	}

	// Create a map to easily store each node as a key value pair
	mutex.Lock()
	member_map := make(map[string]*common.Member)
	// Id to member map
	for i := 0; i < len(member_list); i++ {
		member_map[member_list[i].Id] = member_list[i]
	}
	mutex.Unlock()

	// Loop through incoming member list
	for i := 0; i < len(node_mem_list); i++ {
		// Retrieve current member
		mem, exists := member_map[node_mem_list[i].Id]

		// If it is present in own list, do the following
		if exists {

			// Update heartbeat counter, status and time if incoming list has higher count
			if node_mem_list[i].Hb_counter > mem.Hb_counter {
				member_map[node_mem_list[i].Id].Hb_counter = node_mem_list[i].Hb_counter
				member_map[node_mem_list[i].Id].Status = node_mem_list[i].Status
				member_map[node_mem_list[i].Id].Timestamp = time.Now()
			}

			// The node does not exist in own member list
		} else {
			newMem := node_mem_list[i]

			// If the node is still running, add to our list
			if newMem.Status != common.FAILED {
				newMem.Timestamp = time.Now()
				member_map[newMem.Id] = newMem

				// check if the suspicion node has been added to the membership list
				if newMem.Id == common.SUSPICION_EN {
					sus_mutex.Lock()
					sus_en = true
					sus_mutex.Unlock()
				}

				// if is introducer, add the new member to SDFS
				hostname, _ := os.Hostname()
				if string(hostname) == common.INTRODUCER {
					rqs.EditFCount("add", newMem.Id[:31])
					fmt.Println("updated mem list")
				}
			}
		}

	}

	// Repopulate list of member to reset own member list
	var tmp []*common.Member
	for _, value := range member_map {
		tmp = append(tmp, value)
	}
	mutex.Lock()
	member_list = tmp
	mutex.Unlock()
}

func addMemberToList(conn net.Conn) {
	defer conn.Close()

	// read membership list from the new member(should just be itself)
	buffer := make([]byte, common.BUF_SIZE)
	bytes_read, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Could not add member to the list:", err)
	}
	mutex.Lock()
	node_mem_list, err := unmarshaler(buffer[:bytes_read])
	mutex.Unlock()
	if err != nil {
		fmt.Println("Could not deserialize node's membership list:", err)
	}

	// add new member to own membership list
	mutex.Lock()
	for i := 0; i < len(node_mem_list); i++ {
		// Add mutex here
		newMem := node_mem_list[i]
		newMem.Timestamp = time.Now()
		member_list = append(member_list, newMem)

		// add new member to SDFS
		hostname, _ := os.Hostname()
		if string(hostname) == common.INTRODUCER {
			rqs.EditFCount("add", newMem.Id[:31])
			fmt.Println("updated mem list")
		}
	}

	mutex.Unlock()
}

func gossip(cur_mem *common.Member) {
	t := time.Now()
	mutex.Lock()
	for i := 0; i < len(member_list); i++ {
		// update own heartbeat counter
		if member_list[i].Id == cur_mem.Id {
			member_list[i].Hb_counter += 1
			member_list[i].Timestamp = t
		}

		// update heartbeat counter for suspicion node
		if member_list[i].Id == common.SUSPICION_EN && member_list[i].Status == common.RUNNING {
			member_list[i].Hb_counter += 1
			member_list[i].Timestamp = t
		}

	}

	// find servers to gossip to
	servers := get_available_servers(common.NUM_SERVERS)
	mutex.Unlock()

	// send heartbeats to chosen servers
	for i := 0; i < len(servers); i++ {
		go heartbeat(servers[i])
	}
}

// helper function to heartbeat to specified server
func heartbeat(dest string) {
	mutex.Lock()
	send_list, err := marshaler(member_list)
	mutex.Unlock()
	if err != nil {
		fmt.Println("Could not serialize member list", err)
	}
	dialServer(dest, send_list)
}

func dialServer(dest string, data []byte) {
	// dial a server
	conn, err := net.Dial(common.GOSSIP_CONN_TYPE, dest+":"+common.GOSSIP_PORT)
	if err != nil {
		fmt.Println("Could not dial server", err)
	}
	defer conn.Close()
	// Sending this message to server
	_, err = conn.Write(data)
	if err != nil {
		fmt.Println("Couldn't write this data", err)
	}
}

func checkList() {
	wait_time := common.TGOSSIP

	// loop forever to periodically check the membership list
	for {
		var updated_list []*common.Member
		mutex.Lock()
		for i := 0; i < len(member_list); i++ {
			// determine last update for node in current membership list
			cur_mem := member_list[i]
			last_updated := cur_mem.Timestamp
			t_updated := time.Since(last_updated)
			newt := t_updated.Milliseconds()

			// determine if a running member is still alive
			if cur_mem.Status == common.RUNNING {
				t_fail := (common.TFAIL * time.Second).Milliseconds()
				diff := t_fail - newt
				if diff >= 0 {
					updated_list = append(updated_list, cur_mem)
				} else {
					cur_mem.Status = common.FAILED
					cur_mem.Timestamp = time.Now()
					updated_list = append(updated_list, cur_mem)
				}

				// determine if failed member can be removed from membership list
			} else if cur_mem.Status == common.FAILED {
				t_cleanup := (common.TCLEANUP * time.Second).Milliseconds()
				diff := t_cleanup - newt
				if diff >= 0 {
					updated_list = append(updated_list, cur_mem)
				}

				// make updates for failed node to SDFS
				hostname, _ := os.Hostname()
				if string(hostname) == common.INTRODUCER {
					rqs.EditFCount("delete", cur_mem.Id[:31])
					// re replicate files
					rqs.RereplicateFiles(cur_mem.Id[:31])
					rqs.RemoveSFVM(cur_mem.Id[:31])
					rqs.RemoveFailedVMTasks(cur_mem.Id[:31])
					// rqs.PrintFcount()
				}
			}
		}
		member_list = updated_list
		mutex.Unlock()
		time.Sleep(time.Second * time.Duration(wait_time))
	}
}

// helper function to randomly choose from available servers to gossip to
func get_available_servers(num_servers int) []string {
	var ret []string
	for i := 0; i < len(member_list); i++ {
		if member_list[i].Status == common.FAILED || member_list[i].Id[:31] == host {
			continue
		}
		ret = append(ret, member_list[i].Id[:31])
	}
	if num_servers >= len(ret) {
		return ret
	}
	var new_ret []string
	mems := 0
	for mems < num_servers {
		rand.Seed(time.Now().UnixNano())
		curr_number := rand.Intn(len(ret))
		if !slices.Contains(new_ret, ret[curr_number]) {
			new_ret = append(new_ret, ret[curr_number])
			mems += 1
		}
	}
	// get random indices and make a new ret list that's shuffled
	return new_ret
}

func updateMemberListSuspicion(buffer []byte, bytes_read int) {
	node_mem_list, err := unmarshaler(buffer[:bytes_read])
	if err != nil {
		fmt.Println("Could not deserialize membership list:", err)
	}
	mutex.Lock()
	member_map := make(map[string]*common.Member)
	// id to member map
	var cur_mem *common.Member
	for i := 0; i < len(member_list); i++ {
		member_map[member_list[i].Id] = member_list[i]
		if member_list[i].Id != common.SUSPICION_EN {
			if member_list[i].Id[:31] == host {
				cur_mem = member_list[i]
			}
		}
	}
	mutex.Unlock()

	// loop through incoming membership list
	for i := 0; i < len(node_mem_list); i++ {
		mem, exists := member_map[node_mem_list[i].Id]
		if exists {

			// determine if suspicion has failed(should be disabled locally)
			if node_mem_list[i].Id == common.SUSPICION_EN {
				if node_mem_list[i].Status == common.FAILED {
					sus_mutex.Lock()
					sus_en = false
					sus_mutex.Unlock()
					member_map[node_mem_list[i].Id].Status = common.FAILED
				}
			}
			// if someone is suspicious of current vm
			// increment incarnation #
			if node_mem_list[i].Id == cur_mem.Id && node_mem_list[i].Status == common.SUS {
				member_map[cur_mem.Id].Incarnation += 1
				member_map[cur_mem.Id].Status = common.RUNNING
				member_map[cur_mem.Id].Timestamp = time.Now()
				mem = member_map[cur_mem.Id]
			}

			// all cases of failure means failure
			if mem.Status == common.FAILED || node_mem_list[i].Status == common.FAILED {
				if node_mem_list[i].Hb_counter > mem.Hb_counter {
					member_map[node_mem_list[i].Id].Hb_counter = node_mem_list[i].Hb_counter
					member_map[node_mem_list[i].Id].Status = node_mem_list[i].Status
				}

				// if both say they are sus, they must be sus
			} else if mem.Status == common.SUS && node_mem_list[i].Status == common.SUS {
				member_map[node_mem_list[i].Id].Status = common.SUS
				member_map[node_mem_list[i].Id].Incarnation = common.Max(node_mem_list[i].Incarnation, mem.Incarnation)
				member_map[node_mem_list[i].Id].Hb_counter = common.Max(node_mem_list[i].Hb_counter, mem.Hb_counter)

				// if both are running, it must be running
			} else if mem.Status == common.RUNNING && node_mem_list[i].Status == common.RUNNING {
				member_map[node_mem_list[i].Id].Status = common.RUNNING
				if node_mem_list[i].Hb_counter > mem.Hb_counter {
					member_map[node_mem_list[i].Id].Hb_counter = node_mem_list[i].Hb_counter
					member_map[node_mem_list[i].Id].Timestamp = time.Now()
				}
			} else {
				// 1 Suspicious 1 Running(who is more reliable?)
				if mem.Incarnation > node_mem_list[i].Incarnation {
					if mem.Status == common.RUNNING {
						member_map[node_mem_list[i].Id].Timestamp = time.Now()
					}
					member_map[node_mem_list[i].Id].Hb_counter = common.Max(node_mem_list[i].Hb_counter, mem.Hb_counter)
				} else if mem.Incarnation == node_mem_list[i].Incarnation {
					if mem.Status == common.RUNNING && node_mem_list[i].Status == common.SUS {
						member_map[node_mem_list[i].Id].Timestamp = time.Now()
						member_map[node_mem_list[i].Id].Status = node_mem_list[i].Status
						fmt.Println("NODE", node_mem_list[i].Id, "SUSPECTED AT", time.Now().String())
					}
					member_map[node_mem_list[i].Id].Hb_counter = common.Max(node_mem_list[i].Hb_counter, mem.Hb_counter)
				} else {
					member_map[node_mem_list[i].Id].Status = node_mem_list[i].Status
					member_map[node_mem_list[i].Id].Timestamp = time.Now()
					member_map[node_mem_list[i].Id].Hb_counter = common.Max(node_mem_list[i].Hb_counter, mem.Hb_counter)
				}
			}
		} else {
			// they are a new member
			newMem := node_mem_list[i]
			if newMem.Status != common.FAILED {
				newMem.Timestamp = time.Now()
				member_map[newMem.Id] = newMem

			}
		}
	}
	var tmp []*common.Member
	for _, value := range member_map {
		tmp = append(tmp, value)
	}
	mutex.Lock()
	member_list = tmp
	mutex.Unlock()
}

// Remove from the list when there is suspicion
func checkListSuspicion() {
	wait_time := common.TGOSSIP

	// run forever to update list for suspicion
	for {
		var updated_list []*common.Member
		mutex.Lock()
		for i := 0; i < len(member_list); i++ {

			// determine last update from the node in current membership list
			cur_mem := member_list[i]
			last_updated := cur_mem.Timestamp
			t_updated := time.Since(last_updated)
			newt := t_updated.Milliseconds()

			// determine if someone is suspicious
			if cur_mem.Status == common.RUNNING {
				t_sus := (common.TSUS * time.Second).Milliseconds()
				diff := t_sus - newt
				if diff >= 0 {
					updated_list = append(updated_list, cur_mem)
				} else {
					cur_mem.Status = common.SUS
					cur_mem.Timestamp = time.Now()
					fmt.Println("NODE", cur_mem.Id, "SUSPECTED AT", time.Now().String())
					updated_list = append(updated_list, cur_mem)
				}

				// determine if someone has failed
			} else if cur_mem.Status == common.SUS {
				t_fail := (common.TFAIL * time.Second).Milliseconds()
				diff := t_fail - newt
				if diff >= 0 {
					updated_list = append(updated_list, cur_mem)
				} else {
					cur_mem.Status = common.FAILED
					cur_mem.Timestamp = time.Now()
					updated_list = append(updated_list, cur_mem)
				}

				// determine if someone should be removed from the membership list
			} else {
				t_cleanup := (common.TCLEANUP * time.Second).Milliseconds()
				diff := t_cleanup - newt
				if diff >= 0 {
					updated_list = append(updated_list, cur_mem)
				}
			}

		}
		member_list = updated_list
		mutex.Unlock()
		time.Sleep(time.Second * time.Duration(wait_time))
	}
}

func StatusChecker() {
	int_listener, err := net.Listen(common.JOIN_CONN_TYPE, ":"+common.STATUS_PORT)
	if err != nil {
		fmt.Println("Could not connect to server:", err)
	}
	defer int_listener.Close()
	for {
		conn, err := int_listener.Accept()
		if err != nil {
			fmt.Println("Could not accept a request:", err)
			return
		}
		defer conn.Close()
	}
}
