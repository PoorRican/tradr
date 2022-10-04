#!/usr/bin/bash
# setup docker/server instance

sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install tmux -y

# setup TA-lib
sudo apt-get install gcc -y
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz -q -O /tmp/ta-lib-src.tar.gz
cd /tmp
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure --prefix=/usr && make && sudo make install


# setup required packages
sudo apt-get install python3-pip python3-virtualenv -y

cd ~/investr
virtualenv .env
source .env/bin/activate
pip3 install -r requirements.txt
