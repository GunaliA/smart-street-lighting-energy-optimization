import subprocess

print("TEST 1: Python started")

result = subprocess.run(
    ["cmd.exe", "/c", "C:\\hadoop-3.3.1\\bin\\hdfs.cmd", "dfs", "-ls", "/"],
    capture_output=True,
    text=True,
)

print("TEST 2: subprocess finished")
print("RETURN CODE:", result.returncode)
print("STDOUT:", repr(result.stdout))
print("STDERR:", repr(result.stderr))
