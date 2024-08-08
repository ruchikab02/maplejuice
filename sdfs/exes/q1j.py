import sys
import os
params = sys.argv[1:]


filename ="local/"+params[0]
output_file = params[1]
lines = []
with open(filename, 'r') as input_file:
    lines = input_file.readlines()

with open("local/juice_out/" + output_file + ".txt", "a") as file:
        for line in lines:
            file.write(line)



