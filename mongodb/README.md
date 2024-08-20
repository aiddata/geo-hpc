
# MongoDB Server Setup

Prep
- Have IT create new VM
- Have IT open ports on Mongodb server to allow input from geo.aiddata.wm.edu and all HPC servers (prod and dev servers, where applicable)

Setup
- ssh to server
- `sudo su - aiddata`
- `git clone http://github.com/aiddata/geo-hpc.git`
- `mv geo-hpc/mongodb .`
- `rm -r geo-hpc`



- update mongod.conf
    - if mongo was installed by Puppet when IT setup VM, they will need to update it through Puppet (restart mongod before changes take effect)

- `bash setup.sh`

- need to do something with `mongod.service.conf` file?



- `cp mongodb/disable-transparent-hugepages.sh /etc/init.d/disable-transparent-hugepages`
- `sudo chmod 755 /etc/init.d/disable-transparent-hugepages`
- `sudo update-rc.d disable-transparent-hugepages defaults`



Populate DB from Backup

Note:
When restoring DB from a backup, anything that has happened since backup would be lost. So you should pause all jobs, temporarily take down frontend, and doing fresh backup (i.e., just running `backup_mongo_db.sh`) to make sure nothing is lost.

- pause crons
crontab -l > my_cron_backup.txt
crontab -r
crontab my_cron_backup.txt

- turn off website
- backup db

- `mongorestore --gzip --archive=backup_xyz.archive`
- for details see: https://www.mongodb.com/blog/post/archiving-and-compression-in-mongodb-tools

Adjust IP in geo-hpc config, POST script, other places?
Turn on website
turn on hpc crons


Setup Backup Cron (run as aiddata user)
- `crontab mongodb/mongo.crontab`
- `crontab -e` to edit crontab and set branch to master/develop as needed

- create ssh key
    - `ssh-keygen -t rsa -b 4096`
    - save to default location when prompted (press Enter)
    - do not create a password
    - `eval "$(ssh-agent -s)"`
    - `ssh-add ~/.ssh/id_rsa`
- `cat ~/.ssh/id_rsa.pub` to print public key, then copy it manually
- `ssh aiddatageo@vortex.sciclone.wm.edu`
- `nano ~/.ssh/authorized_keys` and paste private key to new line at the end of the file


Stats (run as personal user)
- copy `active` dir from geoquery-stats repo to `~/stats`
- append to ~/.bashrc: `alias s='bash ~/stats/stats.sh'`


# MongoDB Crash Debug Tips

- Activate aiddata user: `sudo su - aiddata`
- First check logs: `/var/log/mongodb/mongodb.log`
- Attempt simple restart: `sudo service mongod restart`
- Confirm all files owned by mongodb user: `ls -l /var/lib/mongodb`
- Correct ownership if needed: `sudo chown -R mongodb:mongodb /var/lib/mongodb`
- Restart again and check logs (debug further based on logs before using next steps)
    - PID file (`/var/run/mongod.pid`, or `/var/run/mongodb/mongod.pid` for newer version) may be missing
    - `sudo touch /var/run/mongod.pid`
    - `sudo chown mongodb:mongodb /var/run/mongod.pid`
    - restart and check logs again
- If nothing is working delete lock files, restart, and check logs
- Consider restoring from backup if still not working
- If absolutely necessary, use mongod repair (mongo_repair.sh script)


