"""
Interface to execute DVC commands.

@Piotr Styczyński 2021
"""
import io
import os
import subprocess
import sys
import threading
from io import StringIO

try:
    from dvc.main import main as call_dvc_main
except ModuleNotFoundError:
    call_dvc_main = None


from typing import Callable, List, Optional


def get_sys_exit_noop(
    original_callback: Callable[[int], None]
) -> Callable[[int], None]:
    """
    Create fake handler for system exit to prevent command line from killing Python
    process by a mistake.

    :param original_callback: original sys.exit handler
    :returns: Dummy sys.exit handler
    """

    def sys_exit_noop(dumb_code=0):
        nonlocal original_callback
        if dumb_code != 0:
            print(
                "Invalid exit code was returned by the DVC. The program will be terminated."
            )
            original_callback(dumb_code)
        return None

    return sys_exit_noop


class DVCLocalCli:
    """
    DVC low-level command interface.
    """

    working_path: str

    def __init__(self, path: str):
        """
        Create DVC interface for a given working directory.
        The working directory should be existing, configured DVC repo.

        :param path: Path to existing, local, cloned DVC repo
        """
        self.working_path = path

    def _execute_call(
        self,
        args: List[str],
        path: Optional[str] = None,
        collect_output: bool = False,
        input: Optional[str] = None,
        use_shell: bool = True,
        spawn_process: bool = False,
    ) -> str:
        """
        Helper method to execute dvc command.
        :param args: Command arguments
        :param path: Override working path for the command
        :param collect_output: Should return output as a string?
        :param input: Optional stdin input as string
        :param use_shell: Use shell dvc command rather than DVC Python package
        :param spawn_process: Spawn new process to invoke the command (useful when sys.exit is glitched)
        :returns: Output of the command as string (stdout) or empty string if collect_output is False
        """

        if path is None:
            path = self.working_path

        if call_dvc_main is None:
            use_shell = True

        if use_shell:
            cmd = " ".join(["dvc", *args])
            print(f"Spawn process (DVC): {cmd}")
            p = subprocess.Popen(cmd, shell=True, cwd=path)
            p.communicate()
            if p.returncode != 0:
                raise Exception("Error running DVC")
            return ""
        elif spawn_process:
            print("Spawn process (DVC)")
            t = threading.Thread(
                target=self._execute_call,
                args=(args,),
                kwargs=dict(
                    path=path,
                    collect_output=collect_output,
                    input=input,
                    spawn_process=False,
                ),
            )
            t.start()
            t.join()
            print("Process finished (DVC)")
            return ""

        cwd = os.getcwd()
        os.chdir(path)
        call_args = args
        sys.argv = call_args
        print(
            "Running DVC command: {} (path: {})".format(
                " ".join(sys.argv), path
            )
        )

        stdout = sys.stdout
        stdin = sys.stdin
        fake_stdout = None
        sysexit = sys.exit

        if collect_output:
            fake_stdout = io.StringIO("")
            sys.stdout = fake_stdout

        if input is not None:
            stream = StringIO()
            stream.write(input)
            stream.seek(0)
            sys.stdin = stream

        sys.exit = get_sys_exit_noop(sysexit)

        res = call_dvc_main(call_args)
        sys.stdout = stdout
        sys.stdin = stdin
        sys.exit = sysexit
        os.chdir(cwd)

        if collect_output:
            res = fake_stdout.getvalue()

        return res

    def add(self, path: str):
        """
        Add given path to DVC
        :param path: Path to the DVC file
          (note this is path to the DVC metafile so the path should end with .dvc extension)
        """
        self._execute_call(["add", path])

    def pull_path(self, file_path: str):
        """
        Pull DVC repo
        """
        self._execute_call(["pull", file_path])

    def push(self):
        """
        Push DVC changes upstream.
        """
        self._execute_call(["push"])
