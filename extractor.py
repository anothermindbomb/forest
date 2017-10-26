'''
This is extractor for Project forest.

It submits the extraction statements in parallel. Number of concurrent threads is controlled by the value read from the
throttle file in the installation directory.

throttle is re-read periodically, and can be set by the user.
'''

import time
import sys
import subprocess

extractioncommands = r"extractioncommands.txt"
checkpointfile_filename = r"checkpoint.txt"
throttle_filename = r"throttle"
switches = [" /LOG+:robocopy.output", " /NJH", " /NJS", " /NDL", " /NP"]
reportfreq = 100
checksums_required = True
cksum_command = r'C:/cygwin64/bin/sha256sum.exe'
linenumber = 0


def read_throttle():
    throttle_file = open(throttle_filename)
    throttlevalue = int(throttle_file.read())
    throttle_file.close()
    return throttlevalue


if __name__ == '__main__':

    if checksums_required:
        checksum_results = open("checksum_results.txt", mode="w")
    else:
        checksum_results = sys.stdout

    throttle = read_throttle()
    print("Throttle is set to ", throttle)

    checkpoint_file = open(checkpointfile_filename)
    checkpoint_value = int(checkpoint_file.read())
    checkpoint_file.close()
    print("Checkpoint value is ", checkpoint_value)

    if checkpoint_value > reportfreq:  # if we're restarting and we've done more than "reportfreq" files, start from 50 earlier
        checkpoint_value -= 50
        print("Checkpoint restart detected - restarting from", checkpoint_value)

    starttime = time.time()

    pidlist = []

    with open(extractioncommands) as commands:
        while True:
            statement = commands.readline()
            linenumber += 1

            if linenumber <= checkpoint_value:  # skip over lines earlier than our checkpoint
                continue

            if len(statement) == 0:  # skip over any erroneous blank lines
                break

            command = statement.split(" ")[0]
            sourcedirectory = statement.split(" ")[1]
            targetdirectory = statement.split(" ")[2]
            filename = statement.split(" ")[3].rstrip("\n\r")

            if linenumber % 100 == 0:  # take a checkpoint every 100 commands
                checkpoint = open(checkpointfile_filename, mode='w')
                checkpoint.write(str(linenumber))
                checkpoint.close()
                print("Checkpoint taken at line", linenumber)

            # As a bare minimum we need to execute the robocopy command.
            pidlist.append(subprocess.Popen([command, sourcedirectory, targetdirectory, filename, switches],
                                            stdout=subprocess.DEVNULL))
            # We might also need to calculate the checksum of the sourcefile. If this is not required,
            # we can comment these next two lines out.
            if checksums_required:
                cksum_filename = sourcedirectory + "/" + filename
                pidlist.append(subprocess.Popen([cksum_command, cksum_filename], stdout=checksum_results))

            while len(pidlist) >= throttle:
                # check the pidlist, removing any completed processes
                for eachprocess in pidlist:
                    if eachprocess.poll() is not None:
                        pidlist.remove(eachprocess)

                # whilst we seem to be busy, lets see if we have a new throttle value; someone may have changed it.
                oldthrottle = throttle
                throttle = read_throttle()
                if oldthrottle != throttle:
                    print("New throttle detected:", throttle)

            # we'll also check for a new throttle value every "reportfreq" statements, and we'll report
            # on the speed at the same time.
            if linenumber % reportfreq == 0:
                oldthrottle = throttle
                throttle = read_throttle()
                if oldthrottle != throttle:
                    print("New throttle detected:", throttle)

                elapsedtime = time.time() - starttime
                starttime = time.time()
                print(
                    f"Time to submit {reportfreq} files is currently {elapsedtime} seconds. Throttle is {throttle}, and the process list is {len(pidlist)} entries long.")
