<?php

namespace Emartech\Chunkulator\Notifier;

use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;
use Psr\Log\LoggerInterface;

class Worker
{
    private $resourceFactory;


    public function __construct(ResourceFactoryInterface $resourceFactory)
    {
        $this->resourceFactory = $resourceFactory;
    }

    public function run(LoggerInterface $logger): void
    {
        $this->resourceFactory->createQueueFactory()->createNotifierQueue()->consume(
            new Consumer(
                $this->resourceFactory->createResultHandler(),
                $logger
            )
        );
    }
}