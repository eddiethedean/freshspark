import argparse
import code
import sys

from .core import get_fresh_local_spark
from . import reset_active_session, __version__


def cmd_repl(args):
    spark, cleanup = get_fresh_local_spark(
        app_name=args.app_name,
        preset=args.preset,
        reuse_within_process=False,
        print_ui_url=True,
        hive_metastore=args.hive,
        enable_ui=not args.no_ui,
        extra_confs=None,
    )
    banner = (
        f"freshspark REPL (v{__version__})\n"
        f"- app: {args.app_name}\n"
        f"- preset: {args.preset}\n"
        f"- hive_metastore: {args.hive}\n"
        "SparkSession is available as `spark`.\n"
        "Ctrl-D (or exit()) to quit; session will be cleaned."
    )
    try:
        # Print banner explicitly so it shows up even when stdin isn't a TTY (e.g., in tests)
        print(banner, flush=True)
        # Use empty banner for code.interact to avoid double-printing
        code.interact(banner="", local={"spark": spark, "__name__": "__console__"})
    finally:
        cleanup()


def cmd_reset(_args):
    reset_active_session()
    print("freshspark: any active SparkSession has been stopped and gateway closed.")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="freshspark", description="Fresh local Spark utilities")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_repl = sub.add_parser("repl", help="Start a Python REPL with a fresh local SparkSession bound to `spark`")
    p_repl.add_argument("--app-name", default="freshspark", help="Application name (default: freshspark)")
    p_repl.add_argument("--preset", choices=["tiny", "dev", "fat"], default="dev", help="Memory preset")
    p_repl.add_argument("--hive", action="store_true", help="Use isolated embedded Derby metastore (off by default)")
    p_repl.add_argument("--no-ui", action="store_true", help="Disable Spark UI")
    p_repl.set_defaults(func=cmd_repl)

    p_reset = sub.add_parser("reset", help="Stop any active session in this process")
    p_reset.set_defaults(func=cmd_reset)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
