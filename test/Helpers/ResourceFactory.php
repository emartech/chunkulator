<?php

namespace Emartech\Chunkulator\Test\Helpers;

use Emartech\Chunkulator\Calculator\ContactListHandler;
use Emartech\Chunkulator\Calculator\Filter as FilterInterface;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Request\ChunkRequestBuilder;
use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;
use Psr\Log\LoggerInterface;

class ResourceFactory implements ResourceFactoryInterface
{
    private $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function createQueueFactory(): QueueFactory
    {
        return new QueueFactory(
            $this->logger,
            'worker',
            'notifier',
            'amqp://guest:guest@rabbit:5672//',
            1,
            24 * 60 * 60 * 1000
        );
    }

    public function createFilter(): FilterInterface
    {
    }

    public function createResultHandler(): ResultHandler
    {
    }

    public function createContactListHandler(): ContactListHandler
    {
    }
}

