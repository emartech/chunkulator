<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;
use Enqueue\Consumption\QueueConsumer;
use Psr\Log\LoggerInterface;

class Worker
{
    private $resourceFactory;

    public function __construct(ResourceFactoryInterface $resourceFactory)
    {
        $this->resourceFactory = $resourceFactory;
    }

    public function run(LoggerInterface $logger = null): void
    {
        $queueFactory = $this->resourceFactory->createQueueFactory();
        $context = $queueFactory->createContext();

        $consumer = new QueueConsumer($context, null, [], $logger);
        $consumer->bind($queueFactory->getWorkerQueueName(), $this->createChunkProcessor($queueFactory));
        $consumer->consume();
    }

    private function createChunkProcessor(QueueFactory $queueFactory): Consumer
    {
        return new Consumer(
            $this->resourceFactory->createContactListHandler(),
            $this->resourceFactory->createFilter(),
            $this->resourceFactory->createResultHandler(),
            $queueFactory
        );
    }
}
