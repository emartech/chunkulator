<?php

namespace Emartech\Chunkulator;

use Interop\Amqp\AmqpConnectionFactory;
use Interop\Queue\Context;
use Interop\Queue\Queue;
use Psr\Log\LoggerInterface;

class QueueFactory
{
    private $factory;
    private $workerQueueName;
    private $notifierQueueName;
    private $connectionTimeOut;
    private $notificationTTL;
    private $logger;

    public function __construct(LoggerInterface $logger, string $workerQueueName, string $notifierQueueName, AmqpConnectionFactory $factory, int $connectionTimeOut, int $notificationTTL)
    {
        $this->workerQueueName = $workerQueueName;
        $this->notifierQueueName = $notifierQueueName;
        $this->factory = $factory;
        $this->connectionTimeOut = $connectionTimeOut;
        $this->notificationTTL = $notificationTTL;
        $this->logger = $logger;
    }

    public function createContext(): Context
    {
        return $this->factory->createContext();
    }

    public function createWorkerQueue(Context $context): Queue
    {
        $queue = $context->createQueue($this->workerQueueName);
        $context->declareQueue($queue);

        return $queue;
    }

    public function createNotifierQueue(Context $context): Queue
    {
        $queue = $context->createQueue($this->notifierQueueName);
        $queue->setArgument('x-message-ttl', $this->notificationTTL); // use DelayStrategy ?
        $context->declareQueue($queue);

        return $queue;
    }

    public function getWorkerQueueName(): string
    {
        return $this->workerQueueName;
    }

    public function getNotifierQueueName(): string
    {
        return $this->notifierQueueName;
    }
}
