<?php

namespace Emartech\Chunkulator\Test\Unit\Notifier;

use Emartech\Chunkulator\QueueFactory;
use Emartech\TestHelper\BaseTestCase;
use Emartech\Chunkulator\Notifier\ResultHandler;
use Emartech\Chunkulator\Notifier\Calculation;
use Emartech\Chunkulator\Notifier\Consumer;
use Emartech\Chunkulator\Test\Helpers\CalculationRequest;
use Exception;
use Interop\Amqp\AmqpConsumer;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpProducer;
use Interop\Amqp\AmqpQueue;
use PHPUnit\Framework\MockObject\Builder\InvocationMocker;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Rule\InvokedCount;

class CalculationTest extends BaseTestCase
{
    /**
     * @var AmqpMessage|MockObject
     */
    private $message;

    /**
     * @var Consumer|MockObject
     */
    private $consumer;

    /**
     * @var ResultHandler|MockObject
     */
    private $resultHandler;

    /** @var AmqpQueue|MockObject */
    private $notificationQueue;

    /** @var AmqpContext|MockObject */
    private $context;

    /** @var AmqpProducer|MockObject */
    private $producer;

    /** @var AmqpConsumer|MockObject */
    private $amqpConsumer;


    protected function setUp(): void
    {
        parent::setUp();
        $this->message = $this->createMock(AmqpMessage::class);
        $this->consumer = $this->createMock(Consumer::class);
        $this->resultHandler = $this->createMock(ResultHandler::class);
        $this->mockQueues();
    }

    /**
     * @test
     * @dataProvider chunkProvider
     */
    public function allChunksDone_AllTestCases_ReturnsProperBoolean(int $totalChunks, int $finishedChunks, bool $expected)
    {
        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest($totalChunks, 1));

        for ($i = 0; $i < $finishedChunks; $i++) {
            $calculation->addFinishedChunk($i, $this->message);
        }

        $this->assertEquals($expected, $calculation->allChunksDone());
    }

    public function chunkProvider()
    {
        return [
            'single chunk not finished' => [
                'totalChunks' => 1,
                'finishedChunks' => 0,
                'allChunksDone' => false,
            ],
            'single chunk finished' => [
                'totalChunks' => 1,
                'finishedChunks' => 1,
                'allChunksDone' => true,
            ],
            'more chunks but one is missing' => [
                'totalChunks' => 3,
                'finishedChunks' => 2,
                'allChunksDone' => false,
            ],
            'more chunks all finished' => [
                'totalChunks' => 2,
                'finishedChunks' => 2,
                'allChunksDone' => true,
            ],
        ];
    }

    /**
     * @test
     */
    public function finish_Finished_ResumeTriggeredMessageAcked()
    {
        $this->expectSuccessHandlerCall();

        $this->amqpConsumer->expects($this->once())->method('acknowledge')->with($this->message);

        $calculation = new Calculation($this->resultHandler, CalculationRequest::createCalculationRequest(1, 1));
        $calculation->addFinishedChunk(0, $this->message);

        $calculation->finish($this->amqpConsumer, $this->consumer);
    }

    private function expectSuccessHandlerCall(InvokedCount $invocationRule = null): InvocationMocker
    {
        return $this->resultHandler->expects($invocationRule ?? $this->once())->method('onSuccess');
    }

    private function mockQueues(): QueueFactory
    {
        $this->notificationQueue = $this->createMock(AmqpQueue::class);
        $this->context = $this->createMock(AmqpContext::class);
        $this->producer = $this->createMock(AmqpProducer::class);
        $this->amqpConsumer = $this->createMock(AmqpConsumer::class);

        $queueFactory = $this->createMock(QueueFactory::class);
        $queueFactory
            ->expects($this->any())
            ->method('createNotifierQueue')
            ->willReturn($this->notificationQueue);

        $queueFactory
            ->expects($this->any())
            ->method('createContext')
            ->willReturn($this->context);

        $this->context
            ->expects($this->any())
            ->method('createProducer')
            ->willReturn($this->producer);

        $this->context
            ->expects($this->any())
            ->method('createConsumer')
            ->willReturn($this->amqpConsumer);

        return $queueFactory;
    }
}
