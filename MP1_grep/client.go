package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	PORT      = "8079"
	CONN_TYPE = "tcp"
)

func main() {
	// Parse command line
	user_input := os.Args[1:]

	// Users must provide a command to run along with any requirements for that command(only grep and pattern functionality so far)
	if len(user_input) < 2 {
		fmt.Println("Needs more arguments: provide command to run")
		os.Exit(1)
	}

	// Obtain input from user and serialize it to be sent to server, while rejecting any invalid inputs
	var u []byte
	var err error
	if user_input[0] == "grep" {
		if len(user_input[1:]) > 4 {
			fmt.Println("This is not a valid pattern for the grep command")
			os.Exit(1)
		}
		u, err = json.Marshal(user_input)
		if err != nil {
			fmt.Println("Error while serializing", err)
		}

	} else {
		fmt.Println("This can only run grep commands")
		os.Exit(1)
	}

	// All VMs to be run as servers for the client to connect to
	all_servers := []string{"fa23-cs425-3401.cs.illinois.edu",
		"fa23-cs425-3402.cs.illinois.edu",
		"fa23-cs425-3403.cs.illinois.edu",
		"fa23-cs425-3404.cs.illinois.edu",
		"fa23-cs425-3405.cs.illinois.edu",
		"fa23-cs425-3406.cs.illinois.edu",
		"fa23-cs425-3407.cs.illinois.edu",
		"fa23-cs425-3408.cs.illinois.edu",
		"fa23-cs425-3409.cs.illinois.edu",
		"fa23-cs425-3410.cs.illinois.edu"}

	// Create the channel to concurrently retrieve and store inputs from servers
	server_outputs := make(chan []string)

	// Make calls to servers to grep by multithreading via goroutine
	for i := 0; i < len(all_servers); i++ {
		go callServer(all_servers[i], u, server_outputs)
	}

	// Compile all outputs needed for client and send them to client's terminal
	printOutput(all_servers, server_outputs)
}

func printOutput(all_servers []string, server_outputs chan []string) {
	// Counter for total matches in all files
	total_counts_from_servers := 0

	// Go through channel for all server outputs
	for i := 0; i < len(all_servers); i++ {
		all_server_match_counts := <-server_outputs

		for _, dat := range all_server_match_counts[1:] {

			// Post-processing to obtain per server counts and add to total program count
			dat = strings.TrimSpace(dat)
			parts := strings.Split(dat, ":")
			lines_read_int, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
			total_counts_from_servers += lines_read_int

			// Print current server output to terminal
			fmt.Println(dat)
		}
	}

	// Print total count from all servers to terminal
	fmt.Println("Total lines matched from all functional servers:", total_counts_from_servers)
}

func callServer(cur_server string, serialized_input []byte, server_outputs chan []string) {
	// Resource: https://www.educative.io/answers/what-is-the-netdialtimeout-function-in-golang
	// Attempt to connect to a server,timeout for when servers may not be running and we try to connect
	conn, err := net.DialTimeout(CONN_TYPE, cur_server+":"+PORT, 10*time.Second)

	// Create list for current server's output to be added to channel
	result := make([]string, 1)

	// Format and append output when connection to server fails for the specific machine
	if err != nil {
		vm_num, _ := strconv.Atoi(string(cur_server[13:15]))
		result = append(result, "vm"+strconv.Itoa(vm_num)+": Could not connect to server with error: "+err.Error())
		server_outputs <- result
		return
	}

	// Write the pattern to the servers
	_, err = conn.Write(serialized_input)

	// Format and append output when writing to server fails for the specific machine
	if err != nil {
		vm_num, _ := strconv.Atoi(string(cur_server[13:15]))
		result = append(result, "vm"+strconv.Itoa(vm_num)+": Could not write to server with error: "+err.Error())
		server_outputs <- result
		return
	}
	defer conn.Close()

	// Create buffer and read in from connection to the server
	server_counts := make([]byte, 1024)
	bytes_read, err := conn.Read(server_counts)

	// Format and append output when reading from the server fails for the specific machine
	if err != nil {
		vm_num, _ := strconv.Atoi(string(cur_server[13:15]))
		result = append(result, "vm"+strconv.Itoa(vm_num)+": Could not read from server with error: "+err.Error())
		server_outputs <- result
		return
	}

	// Append the result from the server and add it to the channel
	result = append(result, string(server_counts[:bytes_read]))
	server_outputs <- result
	return
}
