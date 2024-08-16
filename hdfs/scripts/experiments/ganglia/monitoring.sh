sudo apt-get update
sudo apt-get install -y ganglia-monitor rrdtool gmetad ganglia-webfrontend apache2
sudo cp /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia.conf
#sudo service apache2 stop
#sudo service ganglia-monitor stop
#sudo service gmetad stop

sudo useradd -d /var/lib/ganglia -s /sbin/nologin ganglia
sudo chown ganglia:ganglia /var/lib/ganglia

sudo cp -r /usr/share/ganglia-webfrontend /var/www/html/ganglia

sudo cp gmetad.conf /etc/ganglia/gmetad.conf
sudo cp gmond.conf /etc/ganglia/gmond.conf

sudo service apache2 restart
sudo service gmetad restart
sudo service ganglia-monitor restart
