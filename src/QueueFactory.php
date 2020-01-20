<?php

namespace Emartech\Chunkulator;

use Emartech\AmqpWrapper\Factory;
use Emartech\AmqpWrapper\Queue;
use Psr\Log\LoggerInterface;

class QueueFactory
{
    private $factory;
    private $workerQueueName;
    private $notifierQueueName;
    private $connectionUrl;
    private $connectionTimeOut;
    private $notificationTTL;
    private $logger;

    public function __construct(LoggerInterface $logger, string $workerQueueName, string $notifierQueueName, string $connectionUrl, int $connectionTimeOut, int $notificationTTL)
    {
        $this->workerQueueName = $workerQueueName;
        $this->notifierQueueName = $notifierQueueName;
        $this->connectionUrl = $connectionUrl;
        $this->connectionTimeOut = $connectionTimeOut;
        $this->notificationTTL = $notificationTTL;
        $this->logger = $logger;
    }

    public function createWorkerQueue(): Queue
    {
        return $this->getConnectionPool()->createQueue($this->workerQueueName);
    }

    public function createNotifierQueue(): Queue
    {
        return $this->getConnectionPool()
            ->createQueue($this->notifierQueueName, $this->notificationTTL);
    }

    private function getConnectionPool(): Factory
    {
        if (!isset($this->factory)) {
            $this->factory = new Factory($this->logger, $this->connectionUrl, $this->connectionTimeOut);
        }
        return $this->factory;
    }

    public function closeNotifierQueue()
    {
        return $this->getConnectionPool()->closeQueue($this->notifierQueueName);
    }
}
