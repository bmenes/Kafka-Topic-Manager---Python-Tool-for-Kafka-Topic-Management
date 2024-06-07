from confluent_kafka.admin import (AdminClient, NewTopic, 
                                   ConfigResource)
from config import config


def topic_exists(admin,topic):

    metadata=admin.list_topics()

    for t in iter(metadata.topics.values()):
        if t.topic==topic:
            return True
        
        else:
            return False






def create_topic(admin,topic,num_partitions=1,replication_factor=1):
    new_topic=NewTopic(topic,num_partitions,replication_factor)
    result_dict=admin.create_topics([new_topic])
    for topic,future in result_dict.items():
        try:
            future.result()
            print(f'Topic {topic} created.')
        

        except Exception as e:
            print(f"Failed to create topic {topic}:{e}")




    



if __name__ == '__main__':
    
    admin=AdminClient(config)
    topic_name="example-topic"
    max_msg_k=50

    if not topic_exists(admin,topic_name):
        create_topic(admin,topic_name)







