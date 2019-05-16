<?php

namespace OldSound\RabbitMqBundle\RabbitMq;

use OldSound\RabbitMqBundle\Serializer\NativeSerializer;
use OldSound\RabbitMqBundle\Serializer\SerializerInterface;
use PhpAmqpLib\Message\AMQPMessage;

class RpcServer extends BaseConsumer
{
	/**
	 * @var SerializerInterface
	 */
    protected $serializer;

    public function initServer($name)
    {
        $this->setExchangeOptions(array('name' => $name, 'type' => 'direct'));
        $this->setQueueOptions(array('name' => $name . '-queue'));
    }

    public function processMessage(AMQPMessage $msg)
    {
        try {
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);

	        if (null !== $this->serializer) {
	            $msg->setBody($this->serializer->deserialize($msg->getBody(), $msg->delivery_info['class'] ?? null));
            }

            $result = call_user_func($this->callback, $msg);

	        if (null !== $this->serializer) {
		        $result = $this->serializer->serialize($result);
	        }

            $this->sendReply($result, $msg->get('reply_to'), $msg->get('correlation_id'));
            $this->consumed++;
            $this->maybeStopConsumer();
        } catch (\Exception $e) {
            $this->sendReply('error: ' . $e->getMessage(), $msg->get('reply_to'), $msg->get('correlation_id'));
        }
    }

    protected function sendReply($result, $client, $correlationId)
    {
        $reply = new AMQPMessage($result, array('content_type' => 'text/plain', 'correlation_id' => $correlationId));
        $this->getChannel()->basic_publish($reply, '', $client);
    }

    public function setSerializer(SerializerInterface $serializer)
    {
        $this->serializer = $serializer;
    }

	public function getSerializer()
	{
		if (null === $this->serializer) {
			$this->serializer = new NativeSerializer();
		}

		return $this->serializer;
	}
}
