import pika, data_manager

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.queue_declare(queue='mychannel')

the_body = ""

def callback(ch, method, properties, body):
    the_body = body    
    print(" [x] Received %r" % body)
    parameters = the_body.split(',')
    print ("the country is %r, the year is %r and the db is %r" % ( parameters[0], parameters[1] , parameters[2]))
    data_manager.main(parameters[0],parameters[1] , parameters[2])

channel.basic_consume(callback,
                      queue='mychannel',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
