#!/usr/bin/env bash

set -eo pipefail
set -x

case $1 in
  post-install)
    echo -e "\nROTONDA VERSION:"
    VER=$(rotonda --version)
    echo $VER

    echo -e "\nROTONDA CONF DIR:"
    ls -lr /etc/rotonda/

    echo -e "\nROTONDA CONF:"
    cat /etc/rotonda/rotonda.conf

    echo -e "\nROTONDA SERVICE STATUS BEFORE ENABLE:"
    systemctl status rotonda || true

    echo -e "\nROTONDA MAN PAGE (first 20 lines only):"
    man -P cat rotonda | head -n 20 || true

    echo -e "\nROTONDA MVP CONFIG DUMP:"
    rotonda --print-config-and-exit
    ;;

  post-upgrade)
    echo -e "\nROTONDA VERSION:"
    rotonda --version

    echo -e "\nROTONDA CONF DIR:"
    ls -lr /etc/rotonda/
    
    echo -e "\nROTONDA CONF:"
    cat /etc/rotonda/rotonda.conf
    
    echo -e "\nROTONDA SERVICE STATUS:"
    systemctl status rotonda || true
    
    echo -e "\nROTONDA MAN PAGE (first 20 lines only):"
    man -P cat rotonda | head -n 20 || true

    echo -e "\nROTONDA MVP CONFIG DUMP:"
    rotonda --print-config-and-exit
    ;;
esac