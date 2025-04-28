import os
import subprocess

def find_files(dirs, exts):
    files = []
    for d in dirs:
        for root, _, filenames in os.walk(d):
            for f in filenames:
                if any(f.endswith(ext) for ext in exts):
                    files.append(os.path.join(root, f))
    return files

def main():
    clang_format = "clang-format"
    # On Windows, try clang-format.exe if clang-format is not found
    if os.name == "nt":
        from shutil import which
        if not which(clang_format):
            clang_format = "clang-format.exe"
    files = find_files(["src", "include"], [".c", ".h"])
    if not files:
        print("No source or header files found.")
        return
    for f in files:
        subprocess.run([clang_format, "-i", f])
    print("Clang-format applied to all source and header files.")

if __name__ == "__main__":
    main()
