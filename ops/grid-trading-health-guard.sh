#!/bin/sh

set -eu

container=${GRID_HEALTH_CONTAINER:-grid-trading-rust}
tag=${GRID_HEALTH_TAG:-grid-trading-health}
state_dir=${GRID_HEALTH_STATE_DIR:-/run/grid-trading-health-guard}
cooldown_seconds=${GRID_HEALTH_ALERT_COOLDOWN_SECONDS:-1800}

mkdir -p "$state_dir"
now=$(date +%s)

alert_limited() {
    severity=$1
    key=$2
    message=$3
    stamp=$state_dir/$key.last
    last=0

    if test -r "$stamp"; then
        read -r last <"$stamp" || last=0
    fi
    case "$last" in
        ''|*[!0-9]*) last=0 ;;
    esac

    if test $((now - last)) -ge "$cooldown_seconds"; then
        logger -p "daemon.$severity" -t "$tag" -- "$message"
        printf '%s\n' "$now" >"$stamp.tmp"
        mv "$stamp.tmp" "$stamp"
    fi
}

clear_alert() {
    rm -f "$state_dir/$1.last"
}

if ! docker inspect "$container" >/dev/null 2>&1; then
    alert_limited err container_missing "container_missing name=$container"
    exit 0
fi
clear_alert container_missing

status=$(docker inspect "$container" --format '{{.State.Status}}')
health=$(docker inspect "$container" --format \
    '{{if .State.Health}}{{.State.Health.Status}}{{else}}not_configured{{end}}')
oom=$(docker inspect "$container" --format '{{.State.OOMKilled}}')
restarts=$(docker inspect "$container" --format '{{.RestartCount}}')

if test "$status" != running || test "$health" != healthy; then
    alert_limited err container_unhealthy \
        "container_unhealthy name=$container status=$status health=$health"
else
    clear_alert container_unhealthy
fi

if test "$oom" = true; then
    alert_limited err container_oom_killed \
        "container_oom_killed name=$container restarts=$restarts"
else
    clear_alert container_oom_killed
fi

restart_state=$state_dir/container-restarts.value
previous_restarts=0
if test -r "$restart_state"; then
    read -r previous_restarts <"$restart_state" || previous_restarts=0
fi
case "$previous_restarts" in
    ''|*[!0-9]*) previous_restarts=0 ;;
esac
if test "$restarts" -gt "$previous_restarts"; then
    alert_limited warning container_restarted \
        "container_restarted name=$container previous=$previous_restarts current=$restarts"
fi
printf '%s\n' "$restarts" >"$restart_state"

container_pid=$(docker inspect "$container" --format '{{.State.Pid}}')
cgroup_path=$(awk -F: '$1 == "0" { print $3 }' "/proc/$container_pid/cgroup" 2>/dev/null || true)
memory_file=/sys/fs/cgroup$cgroup_path/memory.current
if test -n "$cgroup_path" && test -r "$memory_file"; then
    memory_mib=$(( $(cat "$memory_file") / 1048576 ))
    if test "$memory_mib" -ge 1200; then
        alert_limited warning container_memory_high \
            "container_memory_high name=$container memory_mib=$memory_mib threshold_mib=1200"
    else
        clear_alert container_memory_high
    fi
fi

available_mib=$(( $(awk '/^MemAvailable:/{print $2}' /proc/meminfo) / 1024 ))
if test "$available_mib" -le 384; then
    clear_alert host_memory_low
    alert_limited err host_memory_critical \
        "host_memory_critical available_mib=$available_mib threshold_mib=384"
elif test "$available_mib" -le 768; then
    clear_alert host_memory_critical
    alert_limited warning host_memory_low \
        "host_memory_low available_mib=$available_mib threshold_mib=768"
else
    clear_alert host_memory_critical
    clear_alert host_memory_low
fi

swap_in=$(awk '$1 == "pswpin" { print $2 }' /proc/vmstat)
swap_out=$(awk '$1 == "pswpout" { print $2 }' /proc/vmstat)
swap_state=$state_dir/swap-pages.value
if test -r "$swap_state"; then
    read -r previous_swap_in previous_swap_out <"$swap_state" || true
    previous_swap_in=${previous_swap_in:-$swap_in}
    previous_swap_out=${previous_swap_out:-$swap_out}
    swap_delta=$((swap_in - previous_swap_in + swap_out - previous_swap_out))
    if test "$swap_delta" -ge 256 && test "$available_mib" -le 1024; then
        alert_limited warning host_swap_pressure \
            "host_swap_pressure page_delta=$swap_delta available_mib=$available_mib interval_seconds=60"
    else
        clear_alert host_swap_pressure
    fi
fi
printf '%s %s\n' "$swap_in" "$swap_out" >"$swap_state"

disk_percent=$(df -P / | awk 'NR == 2 { gsub(/%/, "", $5); print $5 }')
if test "$disk_percent" -ge 90; then
    clear_alert root_disk_high
    alert_limited err root_disk_critical \
        "root_disk_critical used_percent=$disk_percent threshold_percent=90"
elif test "$disk_percent" -ge 80; then
    clear_alert root_disk_critical
    alert_limited warning root_disk_high \
        "root_disk_high used_percent=$disk_percent threshold_percent=80"
else
    clear_alert root_disk_critical
    clear_alert root_disk_high
fi

if test -r /proc/pressure/memory && awk '
    $1 == "full" {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^avg60=/) { split($i, value, "="); exit !(value[2] >= 1.0) }
        }
    }
' /proc/pressure/memory; then
    alert_limited warning host_memory_pressure \
        "host_memory_pressure full_avg60_at_least_percent=1"
else
    clear_alert host_memory_pressure
fi

if test -r /proc/pressure/io && awk '
    $1 == "full" {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^avg60=/) { split($i, value, "="); exit !(value[2] >= 5.0) }
        }
    }
' /proc/pressure/io; then
    alert_limited warning host_io_pressure \
        "host_io_pressure full_avg60_at_least_percent=5"
else
    clear_alert host_io_pressure
fi

exit 0
