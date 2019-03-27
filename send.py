import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='mychannel')

country = "Argentina"
year = "2013"
sqldb = "C:\\sqlite\db\chinook.db"
the_body = country + "," + year + "," + sqldb

channel.basic_publish(exchange='',
                      routing_key='mychannel',
                      body=the_body)
print(" [x] Sent 'Hello World!'")
connection.close
