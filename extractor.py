"""
This is extractor for Project forest.

It submits the extraction statements in parallel. Number of concurrent threads is controlled by the value read from the
throttle file in the installation directory.

throttle is re-read periodically, and can be set by the user.
"""

import time
import sys
import subprocess
import datetime

extraction_commands = r"extractioncommands.txt"
checkpointfile_filename = r"checkpoint.txt"
throttle_filename = r"throttle"
switches = [" /LOG+:robocopy.output", " /NJH", " /NJS", " /NP", " /R:5"]
# switches = [" /LOG+:robocopy.output"]
reportfreq = 1000
checksums_required = True
cksum_command = r'C:/cygwin64/bin/sha256sum.exe'
linenumber = 0


def read_throttle():
    with open(throttle_filename) as throttle_file:
        throttle_value = 1  # assume the worst case scenario, we drop back to single streaming
        try:
            throttle_value = int(throttle_file.read())
        except:
            pass
    return throttle_value


if __name__ == '__main__':

    print("Starting: {0}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    if checksums_required:
        checksum_results = open("checksum_results.txt", mode="a", newline="\r\n")
        checksum_errors = open("checksum_errors.txt", mode="a", newline="\r\n")
    else:
        checksum_results = sys.stdout
        checksum_errors = sys.stderr

    throttle = read_throttle()
    print("Throttle is set to ", throttle)

    checkpoint_file = open(checkpointfile_filename)
    checkpoint_value = int(checkpoint_file.read())
    checkpoint_file.close()
    print("Checkpoint value is ", checkpoint_value)

    if checkpoint_value > reportfreq:  # if we're restarting and we've done more than "reportfreq" files, start from
        # 100 earlier
        checkpoint_value -= 100
        print("Checkpoint restart detected - restarting from", checkpoint_value)

    start_time = time.time()

    pidlist = []

    with open(extraction_commands) as commands:
        statement = commands.readline()
        while statement:
            linenumber += 1

            if linenumber <= checkpoint_value:  # skip over lines earlier than our checkpoint
                continue

            command = statement.split(" ")[0]
            sourcedirectory = statement.split(" ")[1]
            targetdirectory = statement.split(" ")[2]
            filename = statement.split(" ")[3].rstrip("\n\r")

            if linenumber % 100 == 0:  # take a checkpoint every 100 commands
                checkpoint = open(checkpointfile_filename, mode='w')
                checkpoint.write(str(linenumber))
                checkpoint.close()
                print("{1} Checkpoint taken at line {0}".format(linenumber,
                                                                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            # As a bare minimum we need to execute the robocopy command.
            pidlist.append(subprocess.Popen([command, sourcedirectory, targetdirectory, filename, switches],
                                            stdout=subprocess.DEVNULL))
            # We might also need to calculate the checksum of the sourcefile.
            if checksums_required:
                cksum_filename = sourcedirectory + "/" + filename
                pidlist.append(
                    subprocess.Popen([cksum_command, cksum_filename], stdout=checksum_results, stderr=checksum_errors))

            while len(pidlist) >= throttle:
                # check the pidlist, removing any completed processes
                time.sleep(0.01)
                for eachprocess in pidlist:
                    if eachprocess.poll() is not None:
                        pidlist.remove(eachprocess)

            # we'll also check for a new throttle value every "reportfreq" statements, and we'll report
            # on the submission speed at the same time.
            if linenumber % reportfreq == 0:
                oldthrottle = throttle
                throttle = read_throttle()
                if oldthrottle != throttle:
                    print("New throttle detected:", throttle)

                elapsed_time = time.time() - start_time
                start_time = time.time()
                print("{3} Time to submit {0} jobs was {1} seconds. Throttle is {2}.".format(reportfreq, elapsed_time,
                                                                                             throttle,
                                                                                             datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            statement = commands.readline()
