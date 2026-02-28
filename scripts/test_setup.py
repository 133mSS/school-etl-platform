import subprocess

checks = [
    "docker --version",
    "docker compose version",
    "python --version",
    "pip --version",
    "git --version",
]

for cmd in checks:
    result = subprocess.run(cmd, shell=True)
    print(cmd, "OK" if result.returncode == 0 else "FAILED")