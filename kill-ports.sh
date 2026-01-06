#!/bin/bash

echo "Liberando puertos de Firebase Emulators..."
echo

for port in 3000 4000 4400 4500 5001 8080 8081 9099 9150; do
    echo -n "Puerto $port: "
    # Busca todos los PIDs que usan el puerto
    pids=$(netstat -ano | grep ":$port " | awk '{print $5}' | sort | uniq)
    
    if [ -z "$pids" ]; then
        echo "ya está libre."
    else
        echo "Matando PIDs: $pids"
        # Manda cada PID por separado a taskkill
        for pid in $pids; do
            taskkill //F //PID $pid > /dev/null 2>&1
        done
        echo "Puerto $port liberado."
    fi
done

echo
echo "¡Todos los puertos de Firebase han sido liberados!"
echo "Ya puedes volver a iniciar los emuladores."