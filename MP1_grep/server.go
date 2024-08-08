package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
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

	// Ensure the user inputs the right number of inputs
	if !(len(user_input) != 1) && !(len(user_input) != 5) {
		fmt.Println("Please provide the correct number of arguments", len(user_input))
		os.Exit(1)
	}

	// Take in user input to determine if filepath is provided or random log file needs to be generated
	var filepath string
	if len(user_input) == 5 {
		if user_input[0] != "-r" {
			fmt.Println("This command is not supported")
			os.Exit(1)
		}

		// 1: known string, 2: count of known string, 3: number of lines, 4: filename
		known, err2 := strconv.Atoi(user_input[2])
		num_lines, err3 := strconv.Atoi(user_input[2])
		if err2 != nil || err3 != nil {
			fmt.Println("Conversion error: ", err2)
			os.Exit(1)
		}

		// Send all information from user to randomly generate a log file
		filepath = generateRandomLog(user_input[1], known, num_lines, user_input[4])
	} else {

		// Set the filepath to what was passed in by the user
		filepath = user_input[0]
	}

	// Obtain the filepath from user input and ensure that it is a valid log file
	if strings.Contains(filepath, ".log") != true {
		fmt.Println("Please provide a valid filepath")
		os.Exit(1)
	}

	// Create server listener to be able to listen for client connections and exit with message when connection fails
	listen, err := net.Listen(CONN_TYPE, ":"+PORT)
	if err != nil {
		fmt.Println("Could not connect to server: ", err)
		os.Exit(1)
	}
	defer listen.Close()

	// Continuous loop for server to accept connections from client and
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Could not accept a request: ", err)
			return
		}

		// Send filepath and connection to be read by client
		go readClientInput(conn, filepath)
	}
}

func readClientInput(conn net.Conn, filepath string) {
	defer conn.Close()

	// Read from client into buffer and print error when reading is not possible
	buffer := make([]byte, 1024)
	bytes_read, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Could not read from client: ", err)
	}

	// Obtain pattern from client and send to grep function
	var user_input []string
	err1 := json.Unmarshal(buffer[:bytes_read], &user_input)
	if err1 != nil {
		fmt.Println("Deserializing error:", err1)
	}
	count_from_server := grep(user_input, filepath)

	// Write counts from server to the client
	conn.Write([]byte(count_from_server))
	return
}

func grep(user_input []string, filepath string) string {
	// Resource: https://www.sohamkamani.com/golang/exec-shell-command/
	// Get the number of the current machine based on host name
	name, _ := os.Hostname()
	vm_num, _ := strconv.Atoi(string(name[13:15]))

	// Check number of parameters and run grep to get output depending on parameters provided
	var lines_count []byte
	var err error
	if len(user_input) == 2 {
		cmd := exec.Command("grep", "-c", user_input[1], filepath)
		lines_count, err = cmd.CombinedOutput()
		if err != nil && lines_count == nil {
			fmt.Println("Could not run command: ", err)
		}
	} else if len(user_input) == 3 {
		cmd := exec.Command("grep", user_input[1], user_input[2], filepath)
		lines_count, err = cmd.CombinedOutput()
		if err != nil && lines_count == nil {
			fmt.Println("Could not run command: ", err)
		}
	} else if len(user_input) == 4 {
		cmd := exec.Command("grep", user_input[1], user_input[2], user_input[3], filepath)
		lines_count, err = cmd.CombinedOutput()
		if err != nil && lines_count == nil {
			fmt.Println("Could not run command: ", err)
		}
	} else {

		// Indicate to client that grep failed
		return "vm" + strconv.Itoa(vm_num) + ": grep failure"
	}
	// Run grep command with count and regex option with given pattern and filepath passed in
	return "vm" + strconv.Itoa(vm_num) + ": " + string(lines_count)
}

func generateRandomLog(pattern string, occurrences int, num_lines int, filepath string) string {
	// Setup for generating various components of log file contents
	file_contents := ""
	commands := []string{"GET ", "POST ", "PUT ", "DELETE "}
	line_length := 120
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ$590()?!@#")
	sandwich := rand.Intn(line_length - len(pattern))
	count := 0
	i := 0

	// Loop until number of lines is reached and count has reached number of desired occurrences
	for i < num_lines || count < occurrences {

		// Randomly determine if line includes pattern and which command to append to string
		rand.Seed(time.Now().UnixNano())
		rand_num := rand.Intn(num_lines)
		command_idx := rand.Intn(4)

		// Populate string for a given line based on whether or not it includes the pattern
		ret_string := ""
		if rand_num == 1 && count < occurrences {

			// Resource: https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
			// Generate random sequence of character exluding space for the pattern to be inserted
			b := make([]rune, (line_length - len(pattern)))
			for j := range b {
				b[j] = letterRunes[rand.Intn(len(letterRunes))]
			}

			// Sandwich the pattern between random sequences of character
			count += 1
			random_string := string(b)
			ret_string += commands[command_idx] + random_string[:sandwich] + pattern + random_string[sandwich:] + "\n"
		} else {
			b := make([]rune, (line_length))
			for j := range b {
				b[j] = letterRunes[rand.Intn(len(letterRunes))]
			}

			// Randomly generate a whole line of random characters
			ret_string += commands[command_idx] + string(b) + "\n"
		}

		// Append the line to the file contents
		file_contents += ret_string
		i += 1
	}

	// Create a file with the passed in filepath as the name
	file, err := os.Create(filepath)
	if err != nil {
		fmt.Println("Could not create file: ", err)
		os.Exit(1)
	}
	defer file.Close()

	// Write the randomly generated string of contents to the file
	_, werr := file.WriteString(file_contents)
	if werr != nil {
		fmt.Println("Could not write to file: ", werr)
		os.Exit(1)
	}

	// Return the name of the filepath to be used for grep
	return filepath
}
