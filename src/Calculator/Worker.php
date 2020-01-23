<?php

namespace Emartech\Chunkulator\Calculator;

use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;

class Worker
{
    private $resourceFactory;

    public function __construct(ResourceFactoryInterface $resourceFactory)
    {
        $this->resourceFactory = $resourceFactory;
    }

    public function run(): void
    {
        $queueFactory = $this->resourceFactory->createQueueFactory();
        $queueFactory->createWorkerQueue()
            ->consume(
                $this->createChunkProcessor($queueFactory)
            );
        $queueFactory->closeWorkerQueue();
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
