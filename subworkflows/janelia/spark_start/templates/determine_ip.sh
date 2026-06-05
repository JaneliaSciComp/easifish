#!/bin/bash
# Determine the IP address of the current host

container_engine=$1

local_ip=
if command -v ipconfig >/dev/null 2>&1; then
    echo "Use ipconfig to get the IP"
    # macOS: resolve the interface of the default route, then ask for its IPv4
    iface=$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')
    local_ip=$(ipconfig getifaddr "$iface")
elif command -v ip >/dev/null 2>&1; then
    echo "Use ip to get the IP"
    # Linux fallback if hostname -I unavailable
    local_ip=$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | tail -1)
else
    # Last resort
    echo "Use ifconfig to get the IP"
    local_ip=$(ifconfig 2>/dev/null | awk '/inet /&&$2!="127.0.0.1"{print $2; exit}')
fi
if [[ -z "${local_ip}" ]] ; then
    if [ "$container_engine" = "docker" ]; then
        for interface in /sys/class/net/{eth*,en*,em*}; do
            [ -e $interface ] && \
            [ `cat $interface/operstate` == "up" ] && \
            local_ip=$(ifconfig `basename $interface` | grep "inet " | awk '$1=="inet" {print $2; exit}' | sed s/addr://g)
            if [[ "$local_ip" != "" ]]; then
                echo "Use IP: $local_ip"
                break
            fi
        done
    fi
    if [[ "$local_ip" == "" ]]; then
        echo "Could not determine local IP: local_ip is empty"
        exit 1
    fi
fi
