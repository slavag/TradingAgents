import subprocess
import sys
import textwrap


def test_web_app_import_does_not_require_sqlite_checkpoint_package():
    script = textwrap.dedent(
        """
        import builtins

        real_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "langgraph.checkpoint.sqlite":
                raise ModuleNotFoundError("No module named 'langgraph.checkpoint.sqlite'")
            return real_import(name, globals, locals, fromlist, level)

        builtins.__import__ = guarded_import
        import tradingagents.web.app
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
