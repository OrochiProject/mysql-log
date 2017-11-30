#! /bin/bash
killall -9 mysqld_safe
killall -9 mysqld
rm -rf /usr/local/mysql
cd mysql-server/
make install
cd /usr/local/mysql
chown -R ylf .
chgrp -R ylf .
scripts/mysql_install_db --user=ylf
chown -R root .
chown -R ylf data
bin/mysqld_safe --user=ylf --transaction-isolation=SERIALIZABLE &
sleep 2
./bin/mysqladmin -u root password 715715
