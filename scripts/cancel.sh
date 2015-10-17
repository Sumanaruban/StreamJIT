# When auto-tuning is going on, we can safely stop the tuning process
# by sending USR1 signal to OpenTuner(streamjit2.py) process.
# Call this script with jobid.stepid of streamjit2.py process
# to send USR1 signal to it. The process will responds to the
# signal by safely terminating the tuning process.
args=("$@")
jobid=${args[0]}
scancel --signal=USR1 $jobid
