import os
import sys
import subprocess


def new_version(old_version, commit_message):
    cm = commit_message.lower()
    [major, minor, fix] = old_version.split(".")
    if "major" in cm:
        return f"{int(major) + 1}.{minor}.{fix}"
    elif "feat" in cm:
        return f"{major}.{int(minor) + 1}.{fix}"
    elif "fix" in cm:
        return f"{major}.{minor}.{int(fix) + 1}"
    else:
        return old_version


if __name__ == "__main__":
    old_version = ""

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--user', 'gitpython'])

    import git
    repo = git.Repo(os.getcwd())
    commit_message = repo.head.reference.commit.message

    with open("version.txt", "r") as r:
        old_version = r.read()
        print(old_version)

    new_version = new_version(old_version, commit_message)
    print(new_version)

    with open("version.txt", "w") as w:
        w.write(new_version)
