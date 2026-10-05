from __future__ import annotations

import shlex


def probe_command(marker: str) -> str:
    probe = (
        f"kill -s 0 -- -$$ && printf ok > {shlex.quote(marker)} "
        f"&& rm -f {shlex.quote(marker)} && test -r /proc/self/stat"
    )
    return f"setsid sh -c {shlex.quote(probe)}; exit $?"


def stop_command(group_id: int, marker: str) -> str:
    return (
        "alive() {\n"
        "  for stat in /proc/[0-9]*/stat; do\n"
        '    IFS= read -r line < "$stat" || continue\n'
        '    fields=${line##*) }\n'
        '    set -- $fields\n'
        f'    if [ "$3" = "{group_id}" ]; then\n'
        '      case "$1" in Z|X) ;; *) return 0 ;; esac\n'
        '    fi\n'
        '  done\n'
        '  return 1\n'
        '}\n'
        f'kill -s KILL -- -{group_id} 2>/dev/null || :\n'
        'n=0\n'
        'while alive; do\n'
        '  n=$((n + 1))\n'
        '  [ "$n" -lt 20 ] || exit 1\n'
        '  sleep 0.05\n'
        'done\n'
        f'rm -f {shlex.quote(marker)}'
    )
