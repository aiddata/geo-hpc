
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

Backup Cron (run as aiddata user)
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

