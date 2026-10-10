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


def episode_owner_command(command: str, control_dir: str, uploaded_script: str) -> str:
    directory = shlex.quote(control_dir)
    launch = shlex.join(["python3", uploaded_script, control_dir, command])
    # The one group leader is a shell only until exec replaces it with Python.
    # PRELAUNCH is removed before Popen eligibility; it is never a drain proof.
    script = (
        f"umask 077; mkdir {directory} || exit 1\n"
        f"mkfifo {directory}/stop || exit 1\n"
        f"exec 3<>{directory}/stop || exit 1\n"
        f"printf 'PRELAUNCH\n' > {directory}/status || exit 1\n"
        f"printf '%s\n' $$ > {directory}/pgid || exit 1\n"
        f"exec {launch}"
    )
    # Preserve workload stderr, suppressing only the waiting shell's kill notice.
    return f"exec 4>&2 2>/dev/null; ( exec setsid sh -c {shlex.quote(script)} 2>&4 4>&- ); exit $?"


def stop_command(control_dir: str, *, episode_owner: bool) -> str:
    directory = shlex.quote(control_dir)
    proof = (
        f'if [ ! -s {directory}/status ]; then exit 76; fi\n'
        f'IFS= read -r state < {directory}/status || exit 76\n'
        f'if [ "$state" != PRELAUNCH ]; then cat {directory}/status; fi'
        if episode_owner else
        f'if [ -s {directory}/status ]; then cat {directory}/status; fi'
    )
    wait_init = '' if episode_owner else 'n=0\n'
    wait_limit = '' if episode_owner else '  n=$((n + 1))\n  [ "$n" -lt 20 ] || exit 1\n'
    return (
        f'if [ ! -s {directory}/pgid ]; then exit 75; fi\n'
        f'IFS= read -r group_id < {directory}/pgid || exit 1\n'
        "alive() {\n"
        "  for stat in /proc/[0-9]*/stat; do\n"
        '    IFS= read -r line 2>/dev/null < "$stat" || continue\n'
        '    fields=${line##*) }\n'
        '    set -- $fields\n'
        '    if [ "$3" = "$group_id" ]; then\n'
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
        + wait_init
        + 'while alive; do\n'
        + wait_limit
        + '  sleep 0.05\n'
        'done\n'
        + proof
    )


def cleanup_command(control_dir: str) -> str:
    directory = shlex.quote(control_dir)
    return f'rm -f {directory}/pgid {directory}/status {directory}/stop && rmdir {directory}'
