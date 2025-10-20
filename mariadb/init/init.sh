echo " ### Setting user privileges ###"

# set permissions for user:pass, then flush to apply
mariadb -uroot -p$MYSQL_ROOT_PASSWORD --execute \
"GRANT ALL PRIVILEGES ON defaultdb.* TO '$MARIADB_USER'@'%' IDENTIFIED BY '$MARIADB_PASSWORD';
GRANT ALL PRIVILEGES ON testing.* TO '$MARIADB_USER'@'%' IDENTIFIED BY '$MARIADB_PASSWORD';
FLUSH PRIVILEGES;"
