<?php

namespace Emartech\Chunkulator\Test;

use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\QueueFactory;
use Emartech\Chunkulator\Test\Helpers\ResourceFactory;
use Interop\Queue\Context;
use Interop\Queue\Queue;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\MockObject;
use Psr\Log\LoggerInterface;
use Emartech\TestHelper\BaseTestCase as HelperBaseTestCase;
use ReflectionObject;
use Throwable;

class IntegrationBaseTestCase extends HelperBaseTestCase
{
    /** @var MockObject|LoggerInterface */
    public $logger;

    /** @var Queue */
    protected $workerQueue;

    /** @var Queue */
    protected $notifierQueue;

    /** @var MemoryHandler */
    private $handler;

    /** @var ResourceFactory */
    protected $resourceFactory;

    /** @var Context */
    protected $context;

    /** @var QueueFactory */
    protected $queueFactory;

    /** @var ResultHandler|MockObject */
    public $resultHandler;

    protected function setUp(): void
    {
        parent::setUp();

        $this->handler = new MemoryHandler(Logger::DEBUG);
        $this->logger = new Logger('test', [$this->handler]);

        $this->resultHandler = $this->createMock(ResultHandler::class);
        $this->resourceFactory = new ResourceFactory($this);
        $this->queueFactory = $this->resourceFactory->createQueueFactory();
        $this->context = $this->queueFactory->createContext();

        $this->cleanupRabbit();

        $this->workerQueue = $this->queueFactory->createWorkerQueue($this->context);
        $this->notifierQueue = $this->queueFactory->createNotifierQueue($this->context);
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
        $workerQueue = $this->queueFactory->createWorkerQueue($this->context);
        $notifierQueue = $this->queueFactory->createNotifierQueue($this->context);
        $this->context->deleteQueue($workerQueue);
        $this->context->deleteQueue($notifierQueue);
    }

    /** @return AmqpMessage[] */
    protected function getMessagesFromQueue(Queue $queue): array
    {
        $consumer = $this->context->createConsumer($queue);

        $messages = [];
        do {
            $message = $consumer->receive(1000); // 1000ms timeout
            if ($message) {
                $messages[] = $message;
            }
        }
        while ($message);

        foreach ($messages as $message) {
            $consumer->reject($message, true);
        }

        return $messages;
    }

    protected function sendMessage(Queue $queue, string $messageBody): void
    {
        $producer = $this->context->createProducer();
        $producer->send(
            $queue,
            $this->context->createMessage($messageBody)
        );
    }
}
