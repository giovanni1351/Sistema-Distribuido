import zmq

context = zmq.Context()

pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5557")

sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5558")

zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
