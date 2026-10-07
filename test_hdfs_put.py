import subprocess
import tempfile
import os

print("TEST 1: creating local file")

with tempfile.NamedTemporaryFile(
    mode="w",
    encoding="utf-8",
    suffix=".json",
    delete=False
) as f:
    f.write('{"test": true}\n')
    local_file = f.name

print("LOCAL FILE:", local_file)

result = subprocess.run(
    [
        "cmd.exe",
        "/c",
        "C:\\hadoop-3.3.1\\bin\\hdfs.cmd",
        "dfs",
        "-put",
        "-f",
        local_file,
        "/smart-street-lighting/stream/python_test.json",
    ],
    capture_output=True,
    text=True,
)

print("RETURN CODE:", result.returncode)
print("STDOUT:", repr(result.stdout))
print("STDERR:", repr(result.stderr))

print("TEST 2: checking HDFS file")

check = subprocess.run(
    [
        "cmd.exe",
        "/c",
        "C:\\hadoop-3.3.1\\bin\\hdfs.cmd",
        "dfs",
        "-ls",
        "/smart-street-lighting/stream",
    ],
    capture_output=True,
    text=True,
)

print("CHECK RETURN CODE:", check.returncode)
print("CHECK STDOUT:", repr(check.stdout))
print("CHECK STDERR:", repr(check.stderr))

os.remove(local_file)
