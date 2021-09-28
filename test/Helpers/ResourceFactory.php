<?php

namespace Emartech\Chunkulator\Test\Helpers;

use Emartech\Chunkulator\Calculator\ContactListHandler;
use Emartech\Chunkulator\Calculator\Filter as FilterInterface;
use Emartech\Chunkulator\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\ResourceFactory as ResourceFactoryInterface;
use Emartech\Chunkulator\Test\IntegrationBaseTestCase;
use Enqueue\AmqpLib\AmqpConnectionFactory;

class ResourceFactory implements ResourceFactoryInterface
{
    private $testCase;
    const QUEUE_NAME_WORKER = 'worker';
    const QUEUE_NAME_NOTIFIER = 'notifier';
    const QUEUE_NAME_ERROR = 'error';

    public function __construct(IntegrationBaseTestCase $testCase)
    {
        $this->testCase = $testCase;
    }

    public function createQueueFactory(): QueueFactory
    {
        return new QueueFactory(
            $this->testCase->logger,
            new AmqpConnectionFactory('amqp://guest:guest@rabbit:5672//'),
            self::QUEUE_NAME_WORKER,
            self::QUEUE_NAME_NOTIFIER,
            self::QUEUE_NAME_ERROR,
            1,
             24 * 60 * 60 * 1000
        );
    }

    public function createFilter(): FilterInterface
    {
    }

    public function createResultHandler(): ResultHandler
    {
        return $this->testCase->resultHandler;
    }

    public function createContactListHandler(): ContactListHandler
    {
    }
}

