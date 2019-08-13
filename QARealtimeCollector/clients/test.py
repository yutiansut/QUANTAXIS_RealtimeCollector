
from QAPUBSUB.consumer import  subscriber
sub = subscriber(host='192.168.2.116',user='admin', password='admin' ,exchange= 'realtime_60min_rb1910')

sub.start()