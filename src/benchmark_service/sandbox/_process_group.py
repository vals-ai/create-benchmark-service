from __future__ import annotations

import shlex


def probe_command(marker: str) -> str:
    probe = (
        f"kill -0 -$$ && mkfifo {shlex.quote(marker)} "
        f"&& exec 3<>{shlex.quote(marker)} && rm -f {shlex.quote(marker)} "
        "&& test -r /proc/self/stat"
    )
    return f"setsid sh -c {shlex.quote(probe)}; exit $?"


def owner_command(command: str, control_dir: str, shell: str) -> str:
    directory = shlex.quote(control_dir)
    # Hold FD 3 before publishing pgid so an early RDWR writer cannot lose
    # its request before the same-group listener starts.
    script = (
        f"umask 077; mkdir {directory} || exit 1\n"
        f"mkfifo {directory}/stop || exit 1\n"
        f"exec 3<>{directory}/stop || exit 1\n"
        f"printf '%s\n' $$ > {directory}/pgid || exit 1\n"
        "( IFS= read -r request <&3; kill -KILL 0 ) >/dev/null 2>&1 &\n"
        f"{shell} {shlex.quote(command)} 3>&-; status=$?\n"
        f"printf '%s\n' \"$status\" > {directory}/status\n"
        "kill -KILL 0"
    )
    # Keep the waiting shell's signal notification off the workload stream;
    # the child restores stderr and closes FD 4 before becoming the owner.
    return f"exec 4>&2 2>/dev/null; ( exec setsid sh -c {shlex.quote(script)} 2>&4 4>&- ); exit $?"


def stop_command(group_id: int, control_dir: str) -> str:
    directory = shlex.quote(control_dir)
    return (
        "alive() {\n"
        "  for stat in /proc/[0-9]*/stat; do\n"
        '    IFS= read -r line 2>/dev/null < "$stat" || continue\n'
        '    fields=${line##*) }\n'
        '    set -- $fields\n'
        f'    if [ "$3" = "{group_id}" ]; then\n'
        '      case "$1" in Z|X) ;; *) return 0 ;; esac\n'
        '    fi\n'
        '  done\n'
        '  return 1\n'
        '}\n'
        'if alive; then\n'
        f'  exec 3<>{directory}/stop || exit 1\n'
        "  printf 'stop\n' >&3 || exit 1\n"
        '  exec 3>&-\n'
        'fi\n'
        'n=0\n'
        'while alive; do\n'
        '  n=$((n + 1))\n'
        '  [ "$n" -lt 20 ] || exit 1\n'
        '  sleep 0.05\n'
        'done\n'
        f'if [ -s {directory}/status ]; then cat {directory}/status; fi'
    )


def cleanup_command(control_dir: str) -> str:
    directory = shlex.quote(control_dir)
    return f'rm -f {directory}/pgid {directory}/status {directory}/stop && rmdir {directory}'
