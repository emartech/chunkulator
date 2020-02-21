<?php

namespace Emartech\Chunkulator\Test;

use Emartech\AmqpWrapper\Queue;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Test\Helpers\ResourceFactory;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\MockObject;
use Psr\Log\LoggerInterface;
use Emartech\TestHelper\BaseTestCase as HelperBaseTestCase;
use ReflectionObject;
use Throwable;

class IntegrationBaseTestCase extends HelperBaseTestCase
{
    /** @var MockObject|LoggerInterface */
    protected $logger;

    /** @var Queue */
    protected $workerQueue;

    /** @var Queue */
    protected $notifierQueue;

    /** @var MemoryHandler */
    private $handler;

    /** @var QueueFactory */
    protected $queueFactory;

    protected function setUp(): void
    {
        parent::setUp();

        $this->handler = new MemoryHandler(Logger::DEBUG);
        $this->logger = new Logger('test', [$this->handler]);

        $resourceFactory = new ResourceFactory($this->logger);
        $this->queueFactory = $resourceFactory->createQueueFactory();
        $this->workerQueue = $this->queueFactory->createWorkerQueue();
        $this->notifierQueue = $this->queueFactory->createNotifierQueue();

        $this->cleanupRabbit();
    }

    protected function onNotSuccessfulTest(Throwable $t): void
    {
        $ro = new ReflectionObject($t);
        $rp = $ro->getProperty('message');
        $rp->setAccessible(true);
        $rp->setValue($t, $t->getMessage() . json_encode($this->handler->getLogs(), JSON_PRETTY_PRINT));
        parent::onNotSuccessfulTest($t);
    }

    protected function tearDown(): void
    {
        $this->cleanupRabbit();

        parent::tearDown();
    }

    private function cleanupRabbit(): void
    {
        $this->workerQueue->purge();
        $this->notifierQueue->purge();
    }

}
