import os
import sys
params = sys.argv[1:]
filename = "local/"+params[0]
output_file = params[1]
count = 0
key = ""
key_v = {}
with open(filename, "r") as file:
    for line in file:
        if count == 0:
            tokens = line.split(":")
            if len(tokens) >= 0:
                key = tokens[0]
            count += 1
    val = str(float(tokens[1].strip()) * 100) + "%"
to_write = key + ":" + val + "\n"
canAppend = True
if os.path.exists("local/juice_out/" + output_file + ".txt"):
    with open("local/juice_out/" + output_file + ".txt", 'r') as file:
        lines = file.readlines()
        if to_write in lines:
            canAppend = False
if canAppend:
    with open("local/juice_out/" + output_file + ".txt", "a") as file:
        file.write(to_write)