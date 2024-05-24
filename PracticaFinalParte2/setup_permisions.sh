#!/bin/bash

# Crear los grupos de usuarios
groupadd ufvauditor
groupadd ufvauditores

# Crear los usuarios y añadirlos a los grupos correspondientes
# Usuarios de sucursales
for i in {1..4}; do
    useradd -m -G ufvauditor userSU00$i
done

# Usuarios adicionales
useradd -m -G ufvauditores userfp
useradd -m -G ufvauditores usermonitor

# Asignar permisos para los usuarios de sucursales
for i in {1..4}; do
    chown userSU00$i:ufvauditor /home/userSU00$i/Desktop/PracticaFinalParte2/carpetacsv/Sucursal$i
    chmod 760 /home/userSU00$i/Sucursal$i
    # Dar permisos de lectura a los demás ficheros de sucursales
    for j in {1..4}; do
        if [ $i -ne $j ]; then
            setfacl -m u:userSU00$i:r /home/userSU00$i/Desktop/PracticaFinalParte2/carpetacsv/Sucursal$i
        fi
    done
done

# Asignar permisos a userfp y usermonitor
chown userfp:ufvauditores /opt/ufvaudit
chown usermonitor:ufvauditores /opt/ufvaudit
chmod 750 /opt/ufvaudit

# Asignar permisos a los archivos en config
chown userfp:ufvauditores /opt/ufvaudit/config.txt
chown usermonitor:ufvauditores /opt/ufvaudit/config.txt
chmod 660 /opt/ufvaudit/conf/config.txt

# Asignar permisos específicos a userfp y usermonitor sobre las aplicaciones
touch /opt/ufvaudit/FileProcessor.c
touch /opt/ufvaudit/Monitoring.c

chown userfp:ufvauditores /opt/ufvaudit/FileProcessor.c
chown usermonitor:ufvauditores /opt/ufvaudit/Monitoring.c

chmod 710 /opt/ufvaudit/FileProcessor.c
chmod 701 /opt/ufvaudit/Monitoring.c

echo "Estructura de usuarios, grupos y permisos creada correctamente."
